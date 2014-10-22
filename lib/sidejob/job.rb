module SideJob
  # Methods shared between {SideJob::Job} and {SideJob::Worker}.
  module JobMethods
    attr_reader :id, :by

    # Sets the job id and clears any cached state.
    # @param id [String]
    # @raise [RuntimeError] Error raised if job id does not exist
    def id=(id)
      @id = id
      reload
      check_exists
    end

    # @return [Boolean] True if two jobs or workers have the same id
    def ==(other)
      other.respond_to?(:id) && @id == other.id
    end

    # @see #==
    def eql?(other)
      self == other
    end

    # @return [Fixnum] Hash value based on the id
    def hash
      @id.hash
    end

    # @return [String] Prefix for all redis keys related to this job
    def redis_key
      "job:#{@id}"
    end
    alias :to_s :redis_key

    # Returns if the job still exists.
    # @return [Boolean] Returns true if this job exists and has not been deleted
    def exists?
      SideJob.redis.hexists 'job', @id
    end

    # Adds a log entry to redis.
    # @param type [String] Log type
    # @param data [Hash] Any extra log data
    # @raise [RuntimeError] Error raised if job no longer exists
    def log(type, data)
      check_exists
      SideJob.redis.lpush "#{redis_key}:log", data.merge(type: type, timestamp: SideJob.timestamp).to_json
    end

    # Return all job logs and optionally clears them.
    # @param clear [Boolean] If true, delete logs after returning them (default false)
    # @return [Array<Hash>] All logs for the job with the newest first
    def logs(clear: false)
      key = "#{redis_key}:log"
      SideJob.redis.multi do |multi|
        multi.lrange key, 0, -1
        multi.del key if clear
      end[0].map {|x| JSON.parse(x)}
    end

    # Retrieve the job's status.
    # @return [String] Job status
    def status
      SideJob.redis.get "#{redis_key}:status"
    end

    # Set the job status.
    # @param status [String] The new job status
    def status=(status)
      SideJob.redis.set "#{redis_key}:status", status
    end

    # Prepare to terminate the job. Sets status to 'terminating'.
    # Then queues the job so that its shutdown method if it exists can be run.
    # After shutdown, the status will be 'terminated'.
    # If the job is currently running, it will finish running first.
    # If the job is already terminated, it does nothing.
    # To start the job after termination, call {#run} with force: true.
    # @param recursive [Boolean] If true, recursively terminate all children (default false)
    # @return [SideJob::Job] self
    def terminate(recursive: false)
      if status != 'terminated'
        self.status = 'terminating'
        sidekiq_queue
      end
      if recursive
        children.each do |child|
          child.terminate(recursive: true)
        end
      end
      self
    end

    # Run the job.
    # This method ensures that the job runs at least once from the beginning.
    # If the job is currently running, it will run again.
    # Just like sidekiq, we make no guarantees that the job will not be run more than once.
    # Unless force is set, if the status is terminating or terminated, the job will not be run.
    # @param force [Boolean] Whether to run if job is terminated (default false)
    # @param at [Time, Float] Time to schedule the job, otherwise queue immediately
    # @param wait [Float] Run in the specified number of seconds
    # @return [SideJob::Job] self
    def run(force: false, at: nil, wait: nil)
      check_exists

      case status
        when 'terminating', 'terminated'
          return unless force
      end

      self.status = 'queued'

      time = nil
      if at
        time = at
        time = time.to_f if time.is_a?(Time)
      elsif wait
        time = Time.now.to_f + wait
      end
      sidekiq_queue(time)

      self
    end

    # Returns all children jobs (unordered).
    # @return [Array<SideJob::Job>] Children jobs
    def children
      SideJob.redis.smembers("#{redis_key}:children").map {|id| SideJob::Job.new(id, by: @by)}
    end

    # Returns all ancestor jobs.
    # @return [Array<SideJob::Job>] Ancestors (parent will be first and root job will be last)
    def ancestors
      SideJob.redis.lrange("#{redis_key}:ancestors", 0, -1).map { |id| SideJob::Job.new(id, by: @by) }
    end

    # Returns the parent job.
    # @return [SideJob::Job, nil] Parent job or nil if none
    def parent
      parent = SideJob.redis.lindex("#{redis_key}:ancestors", 0)
      parent = SideJob::Job.new(parent, by: @by) if parent
      parent
    end

    # Returns if job and all children are terminated.
    # @return [Boolean] True if this job and all children recursively are terminated
    def terminated?
      return false if status != 'terminated'
      children.each do |child|
        return false unless child.terminated?
      end
      return true
    end

    # Deletes the job and all children jobs (recursively) if all are terminated.
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
        multi.hdel 'job', @id
        multi.del ports + %w{status children ancestors log inports:mode outports:mode inports:default outports:default}.map {|x| "#{redis_key}:#{x}" }
      end
      reload
      return true
    end

    # Returns an input port.
    # @param name [Symbol,String] Name of the port
    # @return [SideJob::Port]
    # @raise [RuntimeError] Error raised if port does not exist
    def input(name)
      get_port :in, name
    end

    # Returns an output port
    # @param name [Symbol,String] Name of the port
    # @return [SideJob::Port]
    # @raise [RuntimeError] Error raised if port does not exist
    def output(name)
      get_port :out, name
    end

    # Gets all known input ports.
    # @return [Array<SideJob::Port>] Input ports
    def inports
      SideJob.redis.hkeys("#{redis_key}:inports:mode").map {|name| SideJob::Port.new(self, :in, name)}
    end

    # Gets all known output ports.
    # @return [Array<SideJob::Port>] Output ports
    def outports
      SideJob.redis.hkeys("#{redis_key}:outports:mode").map {|name| SideJob::Port.new(self, :out, name)}
    end

    # Returns some data from the job's state.
    # The job state is cached for the lifetime of the job object. Call {#reload} if the state may have changed.
    # @param key [Symbol,String] Retrieve value for the given key
    # @return [Object,nil] Value from the job state or nil if key does not exist
    # @raise [RuntimeError] Error raised if job no longer exists
    def get(key)
      load_state
      @state[key.to_s]
    end

    # Clears the state and ports cache.
    def reload
      @state = nil
      @ports = nil
    end

    private

    # Queue or schedule this job using sidekiq.
    # @param time [Time, Float, nil] Time to schedule the job if specified
    def sidekiq_queue(time=nil)
      queue = get(:queue)
      klass = get(:class)
      args = get(:args)

      if ! SideJob::Worker.config(queue, klass)
        self.status = 'terminated'
        raise "Worker no longer registered for #{klass} in queue #{queue}"
      end
      item = {'jid' => @id, 'queue' => queue, 'class' => klass, 'args' => args || [], 'retry' => false}
      item['at'] = time if time && time > Time.now.to_f
      Sidekiq::Client.push(item)
    end

    # Returns an input or output port.
    # @param type [:in, :out] Input or output port
    # @param name [Symbol,String] Name of the port
    # @return [SideJob::Port]
    def get_port(type, name)
      port = SideJob::Port.new(self, type, name)
      raise "Unknown #{type}put port: #{name}" unless port.exists?
      port
    end

    # @raise [RuntimeError] Error raised if job no longer exists
    def check_exists
      raise "Job #{@id} no longer exists!" unless exists?
    end

    def load_state
      if ! @state
        state = SideJob.redis.hget('job', @id)
        raise "Job #{@id} no longer exists!" if ! state
        @state = JSON.parse(state)
      end
      @state
    end
  end

  # Wrapper for a job which may not be in progress unlike SideJob::Worker.
  # @see SideJob::JobMethods
  class Job
    include JobMethods

    # @param id [String] Job id
    # @param by [String] By string to store for associating entities to events
    def initialize(id, by: nil)
      @id = id
      @by = by
    end
  end
end
