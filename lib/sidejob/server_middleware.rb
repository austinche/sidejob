module SideJob
  # This middleware is primarily responsible for changing job status depending on events
  # {SideJob::Job} sets status to terminating or queued when a job is queued
  # All other job status changes happen here
  # For simplicity, a job is allowed to be queued multiple times in the Sidekiq queue
  # Only when it gets pulled out to be run, i.e. here, we decide if we want to actually run it
  class ServerMiddleware
    # Default configuration parameters for workers merged with job's config
    DEFAULT_CONFIGURATION = {
        'log_status' => true, # whether to log status changes
        'lock_expiration' => 86400, # the worker should not run longer than this number of seconds
        'max_depth' => 20, # the job should not be nested more than this number of levels
        'max_runs_per_minute' => 60, # generate error if the job is run more often than this
    }

    # Called by sidekiq as a server middleware to handle running a worker
    # @param worker [SideJob::Worker]
    # @param msg [Hash] Sidekiq message format
    # @param queue [String] Queue the job was pulled from
    def call(worker, msg, queue)
      return unless worker.exists? # make sure the job has not been deleted

      last_run = worker.get(:ran_at)

      # we skip the run if we already ran once after the enqueued time
      return if last_run && msg['enqueued_at'] && Time.parse(last_run) > Time.at(msg['enqueued_at'])

      case worker.status
        when 'queued'
          terminate = false
        when 'terminating'
          terminate = true
        else
          # for any other status, we assume this worker does not need to be run
          return
      end

      config = DEFAULT_CONFIGURATION.merge(SideJob::Worker.config(queue, worker.class.name)['worker'] || {})

      # if another thread is already running this job, we don't run the job now
      # this simplifies workers from having to deal with thread safety
      # we will requeue the job in the other thread

      lock = "#{worker.redis_key}:lock"
      now = Time.now.to_f
      val = SideJob.redis.multi do |multi|
        multi.get(lock)
        multi.set(lock, now, {ex: config['lock_expiration']}) # add an expiration just in case the lock becomes stale
      end[0]

      return if val # only run if lock key was not set

      worker.set ran_at: SideJob.timestamp

      # limit each job to being called too many times per minute
      # or too deep of a job tree
      # this is to help prevent bad coding that leads to recursive busy loops
      # Uses Rate limiter 1 pattern from http://redis.io/commands/INCR
      rate_key = "#{worker.redis_key}:rate:#{Time.now.to_i / 60}"
      rate = SideJob.redis.multi do |multi|
        multi.incr rate_key
        multi.expire rate_key, 300 # 5 minutes
      end[0]
      if rate.to_i > config['max_runs_per_minute']
        terminate = true
        worker.log 'error', {error: "Job was terminated due to being called too rapidly"}
      elsif SideJob.redis.llen("#{worker.redis_key}:ancestors") > config['max_depth']
        terminate = true
        worker.log 'error', {error: "Job was terminated due to being too deep"}
      end

      if terminate
        # We let workers perform cleanup before terminating jobs
        # To prevent workers from preventing termination, errors are ignored
        begin
          worker.shutdown if worker.respond_to?(:shutdown)
        rescue => e
          log_exception worker, e
        ensure
          set_status worker, 'terminated', config
          worker.parent.run if worker.parent
        end
      else
        # normal run
        begin
          set_status worker, 'running', config
          yield
          set_status worker, 'completed', config if worker.status == 'running'
        rescue SideJob::Worker::Suspended
          set_status worker, 'suspended', config if worker.status == 'running'
        rescue => e
          set_status worker, 'failed', config if worker.status == 'running'
          log_exception(worker, e)
        ensure
          val = SideJob.redis.multi do |multi|
            multi.get lock
            multi.del lock
          end[0]

          worker.run if val && val.to_f != now # run it again if the lock key changed
          worker.parent.run if worker.parent
        end
      end
    end

    private

    def set_status(worker, status, config)
      worker.set status: status
      worker.log 'status', {status: status} if config['log_status']
    end

    def log_exception(worker, exception)
      # only store the backtrace until the first sidekiq line
      worker.log 'error', {error: exception.message, backtrace: exception.backtrace.take_while {|l| l !~ /sidekiq/}.join("\n")}
    end
  end
end
