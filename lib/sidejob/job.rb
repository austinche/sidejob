module SideJob
  # Methods shared between {SideJob::Job} and {SideJob::Worker}
  module JobMethods
    attr_reader :jid
    attr_reader :by

    # @return [Boolean] True if two jobs or workers have the same jid
    def ==(other)
      other.respond_to?(:jid) && jid == other.jid
    end

    # @see #==
    def eql?(other)
      self == other
    end

    # @return [Fixnum] Hash value based on the jid
    def hash
      jid.hash
    end

    # @return [String] Prefix for all redis keys related to this job
    def redis_key
      "job:#{@jid}"
    end
    alias :to_s :redis_key

    # @return [Boolean] Returns true if this job exists and has not been deleted
    def exists?
      SideJob.redis.exists redis_key
    end

    # Adds a log entry to redis
    # @param type [String] Log type
    # @param data [Hash] Any extra log data
    def log(type, data)
      SideJob.redis.multi do |multi|
        multi.lpush "#{redis_key}:log", data.merge(type: type, timestamp: SideJob.timestamp).to_json
        multi.hset redis_key, :updated_at, SideJob.timestamp.to_json
      end
    end

    # Return all job logs and optionally clears them
    # @param clear [Boolean] If true, delete logs after returning them (default false)
    # @return [Array<Hash>] All logs for the job with the newest first
    def logs(clear: false)
      key = "#{redis_key}:log"
      SideJob.redis.multi do |multi|
        multi.lrange key, 0, -1
        multi.del key if clear
      end[0].map {|x| JSON.parse(x)}
    end

    # Retrieve the job's status
    # @return [String] Job status
    def status
      get(:status)
    end

    # Prepare to terminate the job. Sets status to 'terminating'
    # Then queues the job so that its shutdown method if it exists can be run
    # After shutdown, the status will be 'terminated'
    # If the job is currently running, it will finish running first
    # If the job is already terminated, it does nothing
    # To start the job after termination, call #run with force: true
    # @param recursive [Boolean] If true, recursively terminate all children (default false)
    # @return [SideJob::Job] self
    def terminate(recursive: false)
      if status != 'terminated'
        set status: 'terminating'
        sidekiq_queue
      end
      if recursive
        children.each do |child|
          child.terminate(recursive: true)
        end
      end
      self
    end

    # Run the job
    # This method ensures that the job runs at least once from the beginning
    # If the job is currently running, it will run again
    # Just like sidekiq, we make no guarantees that the job will not be run more than once
    # Unless force is set, if the status is terminating or terminated, the job will not be run
    # @param force [Boolean] Whether to run if job is terminated (default false)
    # @param at [Time, Float] Time to schedule the job, otherwise queue immediately
    # @param wait [Float] Run in the specified number of seconds
    # @return [SideJob::Job] self
    def run(force: false, at: nil, wait: nil)
      time = nil
      if at
        time = at
        time = time.to_f if time.is_a?(Time)
      elsif wait
        time = Time.now.to_f + wait
      end

      case status
        when 'terminating', 'terminated'
          return unless force
      end

      set status: 'queued'
      sidekiq_queue(time)
      self
    end

    # @return [Array<SideJob::Job>] Children jobs
    def children
      SideJob.redis.smembers("#{redis_key}:children").map {|id| SideJob::Job.new(id, by: @by)}
    end

    # @return [Array<SideJob::Job>] Ancestors (parent will be first and root job will be last)
    def ancestors
      SideJob.redis.lrange("#{redis_key}:ancestors", 0, -1).map { |jid| SideJob::Job.new(jid, by: @by) }
    end

    # @return [SideJob::Job, nil] Parent job or nil if none
    def parent
      parent = SideJob.redis.lindex("#{redis_key}:ancestors", 0)
      parent = SideJob::Job.new(parent, by: @by) if parent
      parent
    end

    # @return [Boolean] True if this job and all children recursively are terminated
    def terminated?
      return false if status != 'terminated'
      children.each do |child|
        return false unless child.terminated?
      end
      return true
    end

    # Deletes the job and all children jobs (recursively) if all are terminated
    # @return [Boolean] Whether the job was deleted
    def delete
      return false unless terminated?

      # recursively delete all children first
      children.each do |child|
        child.delete
      end

      # delete all SideJob keys
      ports = inports.map(&:redis_key) + outports.map(&:redis_key)
      SideJob.redis.multi do |multi|
        multi.del ports + [redis_key, "#{redis_key}:children", "#{redis_key}:ancestors", "#{redis_key}:log"]
        multi.srem 'jobs', @jid
      end
      return true
    end

    # Returns an input port
    # @param name [Symbol,String] Name of the port
    # @return [SideJob::Port]
    def input(name)
      get_port :in, name
    end

    # Returns an output port
    # @param name [Symbol,String] Name of the port
    # @return [SideJob::Port]
    def output(name)
      get_port :out, name
    end

    # Gets all known input ports
    # @return [Array<SideJob::Port>] Input ports
    def inports
      (get(:inports) || {}).keys.map {|port| input(port)}
    end

    # Gets all known output ports
    # @return [Array<SideJob::Port>] Output ports
    def outports
      (get(:outports) || {}).keys.map {|port| output(port)}
    end

    # Sets values in the job's state
    # @param data [Hash{String,Symbol => Object}] Data to update: objects should be JSON encodable
    def set(data)
      return unless data.size > 0
      SideJob.redis.hmset redis_key, :updated_at, SideJob.timestamp.to_json, data.map {|key, val| [key, val.to_json]}.flatten(1)
    end

    # Unsets some fields in the job's state
    # @param fields [Array<String,Symbol>] Fields to unset
    def unset(*fields)
      return unless fields.length > 0
      SideJob.redis.multi do |multi|
        multi.hdel redis_key, fields
        multi.hset redis_key, :updated_at, SideJob.timestamp.to_json
      end
    end

    # Loads data from the job's state
    # If only a single field is specified, returns just that value
    # Otherwise returns a hash with all the keys specified
    # @param fields [Array<String,Symbol>] Fields to load or all fields if none specified
    # @return [Hash{String,Symbol => Object},Object] Job state with the fields specified
    def get(*fields)
      data = if fields.length > 0
        values = SideJob.redis.hmget(redis_key, *fields)
        Hash[fields.zip(values)]
      else
        SideJob.redis.hgetall redis_key
      end
      data.merge!(data) {|key, val| val ? JSON.parse("[#{val}]")[0] : val}
      data = data[fields[0]] if fields.length == 1
      data
    end

    # Set port options in the job's state
    # @param type [:in, :out] Input or output port
    # @param name [Symbol,String] Name of the port
    # @param options [Hash] New port options
    def set_port_options(type, name, options)
      key = "#{type}ports"
      ports = get(key) || {}
      ports[name] = options
      set key => ports
    end

    private

    # queue or schedule this job using sidekiq
    # @param time [Time, Float, nil] Time to schedule the job if specified
    def sidekiq_queue(time=nil)
      x = get(:queue, :class)
      queue = x[:queue]
      klass = x[:class]

      if ! SideJob::Worker.config(queue, klass)
        set status: :terminated
        raise "Worker no longer registered for #{klass} in queue #{queue}"
      end
      item = {'jid' => @jid, 'queue' => queue, 'class' => klass, 'args' => [], 'retry' => false}
      item['at'] = time if time && time > Time.now.to_f
      Sidekiq::Client.push(item)
    end

    # Returns an input or output port
    # @param type [:in, :out] Input or output port
    # @param name [Symbol,String] Name of the port
    # @return [SideJob::Port]
    def get_port(type, name)
      name = name.to_s
      ports = get("#{type}ports") || {}
      if ports[name]
        SideJob::Port.new(self, type, name, ports[name])
      else
        # new unknown port so add it with default options
        set_port_options type, name, {}
        SideJob::Port.new(self, type, name)
      end
    end
  end

  # Wrapper for a job which may not be in progress unlike SideJob::Worker
  # @see SideJob::JobMethods
  class Job
    include JobMethods

    # @param jid [String] Job id
    # @param by [String] By string to store for associating entities to events
    def initialize(jid, by: nil)
      @jid = jid
      @by = by
    end
  end
end
