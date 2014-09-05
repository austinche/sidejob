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
      SideJob.redis.exists redis_key
    end

    # @return [Hash] Info hash about the job
    def info
      info = SideJob.redis.hgetall(redis_key)
      return {queue: info['queue'], class: info['class'], args: JSON.parse(info['args']),
              created_at: info['created_at'], updated_at: info['updated_at'],
              restart: info['restart'],
              status: info['status'].to_sym}
    end

    # Sets the job arguments and restarts it
    # @param args [Array<String>] New arguments for the job
    def args=(args)
      SideJob.redis.hset redis_key, 'args', JSON.generate(args)
      restart
    end

    # Adds a log entry to redis
    # @param type [String] Log type
    # @param data [Hash] Any extra log data
    def log(type, data)
      SideJob.redis.lpush "#{redis_key}:log", JSON.generate(data.merge(type: type, timestamp: SideJob.timestamp))
      touch
    end

    # Retrieve the job's status
    # @return [Symbol] Job status
    def status
      st = SideJob.redis.hget(redis_key, 'status')
      st ? st.to_sym : nil
    end

    # Set the job's status
    # @param status [String, Symbol] New status
    def status=(status)
      SideJob.redis.hset redis_key, 'status', status
      log('status', {status: status})
    end

    # Restart the job
    # This method ensures that the job runs at least once from the beginning unless the status is :stopped
    # Therefore, if the job is already running, it will run again
    # If job is already queued or scheduled for an earlier time, this call does nothing
    # @param time [Time, Float, nil] Time to schedule the job if specified
    def restart(time=nil)
      time = time.to_f if time.is_a?(Time)

      info_hash = info

      case info_hash[:status]
        when :queued, :stopped
          # don't requeue already queued job or start a stopped job
          return

        when :running
          # we will requeue the job once the currently running worker completes by SideJob::ServerMiddleware
          SideJob.redis.hset redis_key, :restart, time || 0
          return

        when :scheduled
          # move from scheduled queue to current queue
          job = Sidekiq::ScheduledSet.new.find_job(@jid)
          if job
            if time && Time.at(time).utc > job.at
              # scheduled time is further in the future than currently scheduled run so ignore restart request
              return
            else
              job.delete # Will re-add it below
            end
          end
      end

      if time && time > Time.now.to_f
        self.status = :scheduled
        Sidekiq::Client.push('jid' => @jid, 'queue' => info_hash[:queue], 'class' => info_hash[:class], 'args' => info_hash[:args], 'retry' => false, 'at' => time)
      else
        self.status = :queued
        Sidekiq::Client.push('jid' => @jid, 'queue' => info_hash[:queue], 'class' => info_hash[:class], 'args' => info_hash[:args], 'retry' => false)
      end
    end

    # Restart the job in a certain amount of time
    # @param delta_t [Float]
    def restart_in(delta_t)
      restart(Time.now.to_f + delta_t)
    end

    # @return [Boolean] Return true if this job is restarting
    def restarting?
      SideJob.redis.hexists(redis_key, :restart)
    end

    # @return [Array<SideJob::Job>] Children jobs
    def children
      SideJob.redis.smembers("#{redis_key}:children").map {|id| SideJob::Job.new(id)}
    end

    # @return [Array<SideJob::Job>] Ancestors (parent will be first and root job will be last)
    def ancestors
      SideJob.redis.lrange("#{redis_key}:ancestors", 0, -1).map { |jid| SideJob::Job.new(jid) }
    end

    # @return [SideJob::Job, nil] Parent job or nil if none
    def parent
      parent = SideJob.redis.lindex("#{redis_key}:ancestors", 0)
      parent = SideJob::Job.new(parent) if parent
      parent
    end

    # Deletes and unschedules the job and all children jobs (recursively)
    def delete
      # recursively delete all children first
      children.each do |child|
        child.delete
      end

      # remove from sidekiq queue
      job = Sidekiq::Queue.new(info[:queue]).find_job(@jid)
      job = Sidekiq::ScheduledSet.new.find_job(@jid) if ! job
      job.delete if job

      # delete all SideJob keys
      inports = SideJob.redis.smembers("#{redis_key}:inports").map {|port| "#{redis_key}:in:#{port}"}
      outports = SideJob.redis.smembers("#{redis_key}:outports").map {|port| "#{redis_key}:out:#{port}"}
      SideJob.redis.multi do |multi|
        multi.del inports + outports +
                      [redis_key, "#{redis_key}:inports", "#{redis_key}:outports", "#{redis_key}:children", "#{redis_key}:ancestors", "#{redis_key}:data", "#{redis_key}:log"]
        multi.srem 'jobs', @jid
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

    # Gets all input ports that have data
    # @return [Array<SideJob::Port>] Input ports
    def inports
      SideJob.redis.smembers("#{redis_key}:inports").select do |port|
        SideJob.redis.exists "#{redis_key}:in:#{port}"
      end.map do |port|
        SideJob::Port.new(self, :in, port)
      end
    end

    # Gets all output ports that have data
    # @return [Array<SideJob::Port>] Output ports
    def outports
      SideJob.redis.smembers("#{redis_key}:outports").select do |port|
        SideJob.redis.exists "#{redis_key}:out:#{port}"
      end.map do |port|
        SideJob::Port.new(self, :out, port)
      end
    end

    # Sets multiple values
    # Merges data into a job's metadata
    # @param data [Hash{String => String}] Data to update
    def mset(data)
      SideJob.redis.hmset "#{redis_key}:data", *(data.to_a.flatten(1))
      touch
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
      if fields.length > 0
        values = SideJob.redis.hmget("#{redis_key}:data", *fields)
        Hash[fields.zip(values)]
      else
        SideJob.redis.hgetall "#{redis_key}:data"
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

    # Touch the updated_at timestamp
    def touch
      SideJob.redis.hset redis_key, 'updated_at', SideJob.timestamp
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
