module SideJob
  # Methods shared between SideJob::Job and SideJob::Worker
  module JobMethods
    attr_reader :jid

    def ==(other)
      other.respond_to?(:jid) && jid == other.jid
    end

    def eql?(other)
      self == other
    end

    def hash
      jid.hash
    end

    def redis_key
      "job:#{@jid}"
    end

    # @return [Boolean] Returns true if this job exists and has not been deleted
    def exists?
      SideJob.redis do |redis|
        redis.exists redis_key
      end
    end

    # Queues a child job
    # @see SideJob.queue
    def queue(queue, klass, options={})
      job = SideJob.queue(queue, klass, options)
      job.parent = self
      job
    end

    # Sets multiple values
    # Merges data into a job's metadata
    # @param data [Hash{String => String}] Data to update
    def mset(data)
      SideJob.redis do |redis|
        redis.hmset "#{redis_key}:data", *(data.to_a.flatten(1))
      end
    end

    # Sets a single data in the job's metadata
    # @param field [String,Symbol] Field to set
    # @param value [String]
    def set(field, value)
      mset({field => value})
    end

    # Sets a single JSON encoded data in the job's metadata
    # @param field [String,Symbol] Field to get
    # @param value [Object] JSON-serializable object
    def set_json(field, value)
      return unless value
      set(field, JSON.generate(value))
    end

    # Loads data from the job's metadata
    # @param fields [Array<String,Symbol>] Fields to load or all fields if none specified
    # @return [Hash{String,Symbol => String}] Job's metadata with the fields specified
    def mget(*fields)
      SideJob.redis do |redis|
        if fields.length > 0
          values = redis.hmget("#{redis_key}:data", *fields)
          Hash[fields.zip(values)]
        else
          redis.hgetall "#{redis_key}:data"
        end
      end
    end

    # Gets a single data from the job's metadata
    # @param field [String,Symbol] Field to get
    # @return [String, nil] Value of the given data field or nil
    def get(field)
      mget(field)[field]
    end

    # Gets a single JSON encoded data from the job's metadata
    # @param field [String,Symbol] Field to get
    # @return [Object, nil] JSON parsed value of the given data field
    def get_json(field)
      data = get(field)
      if data
        JSON.parse(data)
      else
        nil
      end
    end

    # Helps with getting and storing configuration-like data from a port
    # The assumption is that a configuration port only cares about the last data received on it
    # The last data is also saved in to the state
    # If no data in on the input port, load from saved state
    # @param field [String,Symbol] Name of configuration field/port
    # @return [String, nil] Configuration value or nil
    def get_config(field)
      port = input(field)
      data = input(field).pop_all.first
      if data
        set(field, data)
      else
        data = get(field)
      end
      data
    end

    # @see #get_config
    # @param field [String,Symbol] Field to get
    # @return [Object, nil] JSON parsed value of the given configuration value
    def get_config_json(field)
      data = get_config(field)
      if data
        JSON.parse(data)
      else
        nil
      end
    end

    # Adds a log entry
    # @param type [String] Log type
    # @param data [Hash] Any extra log data
    def log_push(type, data)
      SideJob.redis do |redis|
        redis.lpush "#{redis_key}:log", JSON.generate(data.merge(type: type, timestamp: Time.now))
      end
    end

    # Pops a log entry
    # @return [Hash]
    def log_pop
      log = SideJob.redis do |redis|
        redis.rpop "#{redis_key}:log"
      end
      log = JSON.parse(log) if log
      log
    end

    # Retrieve the job's status
    # @return [Symbol] Job status
    def status
      st = SideJob.redis do |redis|
        redis.hget "#{redis_key}", 'status'
      end
      st ? st.to_sym : nil
    end

    # Set the job's status
    # If status is set to :completed, :suspended, or :failed, the parent job is restarted
    # @param status [String, Symbol] New status
    def status=(status)
      log_push('status', {status: status})
      SideJob.redis do |redis|
        redis.hset redis_key, 'status', status
      end

      if parent && [:completed, :suspended, :failed].include?(status.to_sym)
        parent.restart
      end
    end

    # Restart the job
    # If the job status is not running (:completed, :suspended, :failed), queues it immediately
    # If the job status is :queued does nothing
    # If the job status is :running, ensures the job will be restarted by SideJob::ServerMiddleware
    def restart
      case status
        when :queued
          # nothing needs to be done
        when :running
          SideJob.redis do |redis|
            redis.hset redis_key, :restart, 1
          end
        when :completed, :suspended, :failed
          original_message = SideJob.redis do |redis|
            redis.hget redis_key, :call
          end
          Sidekiq::Client.push(JSON.parse(original_message))
      end
      self
    end

    # @return [Boolean] Return true if this job is restarting
    def restarting?
      SideJob.redis do |redis|
        redis.hexists(redis_key, :restart)
      end
    end

    # @return [Array<String>] List of children job ids for the given job
    def children
      SideJob.redis do |redis|
        redis.smembers("#{redis_key}:children").map {|id| SideJob::Job.new(id)}
      end
    end

    # @return [SideJob::Job, nil] Parent job or nil if none
    def parent
      return @parent if @parent # parent job will never change
      SideJob.redis do |redis|
        @parent = redis.hget(redis_key, 'parent')
        @parent = SideJob::Job.new(@parent) if @parent
      end
      return @parent
    end

    # Set a job's parent
    # @param parent [SideJob::Job] parent job
    def parent=(parent)
      SideJob.redis do |redis|
        raise 'Cannot change parent job' if redis.hget(redis_key, 'parent')
        redis.multi do |multi|
          multi.hset redis_key, 'parent', parent.jid
          multi.sadd "#{parent.redis_key}:children", @jid
        end
      end
    end

    # Returns the job tree starting from this job
    # @return [Array<Hash>]
    def tree
      children.map do |child|
        { job: child, children: child.tree }
      end
    end

    # Deletes and unschedules the job and all children jobs (recursively)
    def delete
      # recursively delete all children first
      children.each do |child|
        child.delete
      end

      # remove from sidekiq queue
      Sidekiq::Queue.all.each do |queue|
        queue.each do |job|
          job.delete if job.jid == jid
        end
      end

      # delete all SideJob keys
      SideJob::Port.delete_all(self, :in)
      SideJob::Port.delete_all(self, :out)
      SideJob.redis do |redis|
        redis.del [redis_key, "#{redis_key}:children", "#{redis_key}:data", "#{redis_key}:log"]
      end
    end

    # Returns an input port
    # @param port [String] Name of the port
    # @return [SideJob::Port]
    def input(port)
      SideJob::Port.new(self, :in, port)
    end

    # Returns an output port
    # @param port [String] Name of the port
    # @return [SideJob::Port]
    def output(port)
      SideJob::Port.new(self, :out, port)
    end

    # Gets all input ports that have been pushed to
    # @return [Array<SideJob::Port>] Input ports
    def inports
      SideJob::Port.all(self, :in)
    end

    # Gets all output ports that have been pushed to
    # @return [Array<SideJob::Port>] Output ports
    def outports
      SideJob::Port.all(self, :out)
    end

  end

  # Wrapper for a job which may not be in progress unlike SideJob::Worker
  # @see SideJob::JobMethods
  class Job
    include JobMethods

    # @param jid [String] Job id
    def initialize(jid)
      @jid = jid
    end

    def to_s
      @jid
    end
  end
end
