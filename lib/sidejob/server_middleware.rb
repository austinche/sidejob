module SideJob
  # This middleware is primarily responsible for changing job status depending on events
  # {SideJob::Job} sets status to terminating or queued when a job is queued
  # All other job status changes happen here
  # For simplicity, a job is allowed to be queued multiple times in the Sidekiq queue
  # Only when it gets pulled out to be run, i.e. here, we decide if we want to actually run it
  class ServerMiddleware
    class << self
      # If true, we do not rescue or log errors and instead propagate errors (useful for testing)
      attr_accessor :raise_errors
    end

    # Called by sidekiq as a server middleware to handle running a worker
    # @param worker [SideJob::Worker]
    # @param msg [Hash] Sidekiq message format
    # @param queue [String] Queue the job was pulled from
    def call(worker, msg, queue)
      @worker = worker
      return unless @worker.exists? # make sure the job has not been deleted

      # only run if status is queued or terminating
      case @worker.status
        when 'queued', 'terminating'
        else
          return
      end

      # We use the presence of this lock:worker key to indicate that a worker is trying to the get the job lock.
      # No other worker needs to also wait and no calls to {SideJob::Job#run} need to queue a new run.
      return unless SideJob.redis.set("#{@worker.redis_key}:lock:worker", 1, {nx: true, ex: 2})

      # Obtain a lock to allow only one worker to run at a time to simplify workers from having to deal with concurrency
      token = @worker.lock(CONFIGURATION[:lock_expiration])
      if token
        begin
          SideJob.redis.del "#{@worker.redis_key}:lock:worker"
          SideJob.context(job: @worker.id) do
            case @worker.status
              when 'queued'
                run_worker { yield }
              when 'terminating'
                terminate_worker
              # else no longer need running
            end
          end
        ensure
          @worker.unlock(token)
          @worker.run(parent: true) # run the parent every time worker runs
        end
      else
        SideJob.redis.del "#{@worker.redis_key}:lock:worker"
        # Unable to obtain job lock which may indicate another worker thread is running
        # Schedule another run
        # Note that the actual time before requeue depends on sidekiq poll_interval (default 15 seconds)
        case @worker.status
          when 'queued', 'terminating'
            @worker.run(wait: 1)
          # else no longer need running
        end
      end
    end

    private

    def terminate_worker
      # We let workers perform cleanup before terminating jobs
      # To prevent workers from preventing termination, errors are ignored
      @worker.shutdown if @worker.respond_to?(:shutdown)
    rescue => e
      add_exception e
    ensure
      @worker.status = 'terminated'
    end

    def run_worker(&block)
      # limit each job to being called too many times per minute
      # this is to help prevent bad coding that leads to infinite looping
      # Uses Rate limiter 1 pattern from http://redis.io/commands/INCR
      rate_key = "#{@worker.redis_key}:rate:#{Time.now.to_i / 60}"
      rate = SideJob.redis.multi do |multi|
        multi.incr rate_key
        multi.expire rate_key, 60
      end[0]

      if rate.to_i > CONFIGURATION[:max_runs_per_minute]
        SideJob.log({ error: 'Job was terminated due to being called too rapidly' })
        @worker.terminate
      else
        # normal run

        # if ran_at is not set, then this is the first run of the job, so call the startup method if it exists
        @worker.startup if @worker.respond_to?(:startup) && ! SideJob.redis.exists("#{@worker.redis_key}:ran_at")

        SideJob.redis.set "#{@worker.redis_key}:ran_at", SideJob.timestamp
        @worker.status = 'running'
        yield
        @worker.status = 'completed' if @worker.status == 'running'
      end
    rescue SideJob::Worker::Suspended
      @worker.status = 'suspended' if @worker.status == 'running'
    rescue => e
      # only set failed if not terminating/terminated
      case @worker.status
        when 'terminating', 'terminated'
        else
          @worker.status = 'failed'
      end

      add_exception e
    end

    def add_exception(exception)
      if SideJob::ServerMiddleware.raise_errors
        raise exception
      else
        # only store the backtrace until the first sidekiq line
        SideJob.log({ error: exception.message, backtrace: exception.backtrace.take_while {|l| l !~ /sidekiq/}.join("\n") })
      end
    end
  end
end
