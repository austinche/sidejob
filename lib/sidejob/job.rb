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

    # @return [Hash] Info hash about the job
    def info
      info = SideJob.redis do |redis|
        redis.hgetall redis_key
      end
      return {queue: info['queue'], class: info['class'], args: JSON.parse(info['args']),
              parent: info['parent'] ? SideJob::Job.new(info['parent']) : nil, restart: info['restart'],
              status: info['status'].to_sym}
    end

    # Sets the job arguments and restarts it
    # @param args [Array<String>] New arguments for the job
    def args=(args)
      SideJob.redis do |redis|
        redis.hset redis_key, 'args', JSON.generate(args)
      end
      restart
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
        redis.hget redis_key, 'status'
      end
      st ? st.to_sym : nil
    end

    # Set the job's status
    # @param status [String, Symbol] New status
    def status=(status)
      log_push('status', {status: status})
      SideJob.redis do |redis|
        redis.hset redis_key, 'status', status
      end
    end

    # Restart the job
    # This method ensures that the job runs at least once from the beginning
    # Therefore, if the job is already running, it will run again
    # If job is already queued or scheduled for an earlier time, this call does nothing
    # @param time [Time, Float, nil] Time to schedule the job if specified
    def restart(time=nil)
      time = time.to_f if time.is_a?(Time)

      info_hash = info

      case info_hash[:status]
        when :queued
          # don't requeue already queued job
          return

        when :running
          # we will requeue the job once the currently running worker completes by SideJob::ServerMiddleware
          SideJob.redis do |redis|
            redis.hset redis_key, :restart, time || 0
          end
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
      job = Sidekiq::Queue.new(info[:queue]).find_job(@jid)
      job = Sidekiq::ScheduledSet.new.find_job(@jid) if ! job
      job.delete if job

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
