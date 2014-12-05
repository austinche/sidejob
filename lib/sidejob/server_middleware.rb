module SideJob
  # This middleware is primarily responsible for changing job status depending on events
  # {SideJob::Job} sets status to terminating or queued when a job is queued
  # All other job status changes happen here
  # For simplicity, a job is allowed to be queued multiple times in the Sidekiq queue
  # Only when it gets pulled out to be run, i.e. here, we decide if we want to actually run it
  class ServerMiddleware
    # Called by sidekiq as a server middleware to handle running a worker
    # @param worker [SideJob::Worker]
    # @param msg [Hash] Sidekiq message format
    # @param queue [String] Queue the job was pulled from
    def call(worker, msg, queue)
      @worker = worker
      return unless @worker.exists? # make sure the job has not been deleted

      case @worker.status
        when 'queued'
          run_worker { yield }
        when 'terminating'
          terminate_worker
        # for any other status, we assume this worker does not need to be run
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
      @worker.parent.run if @worker.parent
    end

    def run_worker(&block)
      # if another thread is already running this job, we don't run the job now
      # this simplifies workers from having to deal with thread safety
      # we will requeue the job in the other thread
      lock = "#{@worker.redis_key}:lock"
      now = Time.now.to_f
      val = SideJob.redis.multi do |multi|
        multi.get(lock)
        multi.set(lock, now, {ex: CONFIGURATION[:lock_expiration]}) # add an expiration just in case the lock becomes stale
      end[0]

      return if val # only run if lock key was not set

      begin
        # limit each job to being called too many times per minute
        # this is to help prevent bad coding that leads to infinite looping
        # Uses Rate limiter 1 pattern from http://redis.io/commands/INCR
        rate_key = "#{@worker.redis_key}:rate:#{Time.now.to_i / 60}"
        rate = SideJob.redis.multi do |multi|
          multi.incr rate_key
          multi.expire rate_key, 300 # 5 minutes
        end[0]

        if rate.to_i > CONFIGURATION[:max_runs_per_minute]
          SideJob.log({ job: @worker.id, error: 'Job was terminated due to being called too rapidly' })
          @worker.terminate
        else
          # normal run
          @worker.set ran_at: SideJob.timestamp
          @worker.status = 'running'
          yield
          @worker.status = 'completed' if @worker.status == 'running'
        end
      rescue SideJob::Worker::Suspended
        @worker.status = 'suspended' if @worker.status == 'running'
      rescue => e
        @worker.status = 'failed' if @worker.status == 'running'
        add_exception e
      ensure
        val = SideJob.redis.multi do |multi|
          multi.get lock
          multi.del lock
        end[0]

        @worker.run if val && val.to_f != now # run it again if the lock key changed
        @worker.parent.run if @worker.parent
      end
    end

    def add_exception(exception)
      # only store the backtrace until the first sidekiq line
      SideJob.log({ job: @worker.id, error: exception.message, backtrace: exception.backtrace.take_while {|l| l !~ /sidekiq/}.join("\n") })
    end
  end
end
