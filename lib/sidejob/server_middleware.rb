module SideJob
  class ServerMiddleware
    MAX_CALLS_PER_SECOND = 10
    MAX_JOB_DEPTH = 10
    STALE_LOCK_EXPIRE = 86400 # no worker should run longer than this number of seconds

    # This middleware is primarily responsible for changing job status depending on events
    # SideJob::Job sets status to terminating or queued when a job is queued
    # All other job status changes happen here
    # For simplicity, a job is allowed to be queued multiple times in the Sidekiq queue
    # Only when it gets pulled out to be run, i.e. here, we decide if we want to actually run it
    def call(worker, msg, queue)
      last_run = SideJob.redis.hget worker.redis_key, 'ran_at'

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

      SideJob.redis.hset worker.redis_key, 'ran_at', SideJob.timestamp

      # limit each job to being called too many times per second
      # or too deep of a job tree
      # this is to help prevent bad coding that leads to recursive busy loops
      # Uses Rate limiter 1 pattern from http://redis.io/commands/INCR
      rate_key = "#{worker.redis_key}:rate:#{Time.now.to_i}"
      rate = SideJob.redis.multi do |multi|
        multi.incr rate_key
        multi.expire rate_key, 10
      end[0]
      if rate.to_i > MAX_CALLS_PER_SECOND
        terminate = true
        worker.log 'error', {error: "Job was terminated due to being called too rapidly"}
      elsif SideJob.redis.llen("#{worker.redis_key}:ancestors") > MAX_JOB_DEPTH
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
          set_status worker, 'terminated'
          worker.parent.run if worker.parent
        end
      else
        # normal run

        # if another thread is already running this job, we don't run the job now
        # this simplifies workers from having to deal with thread safety
        # we will requeue the job in the other thread

        lock = "#{worker.redis_key}:lock"
        now = Time.now.to_f
        val = SideJob.redis.multi do |multi|
          multi.get(lock)
          multi.set(lock, now, {ex: STALE_LOCK_EXPIRE}) # add an expiration just in case the lock becomes stale
        end[0]

        # only run if lock key was not set
        if ! val
          begin
            set_status worker, 'running'
            Thread.current[:SideJob] = worker
            yield
            set_status worker, 'completed' if worker.status == 'running'
          rescue SideJob::Worker::Suspended
            set_status worker, 'suspended' if worker.status == 'running'
          rescue => e
            set_status worker, 'failed' if worker.status == 'running'
            log_exception(worker, e)
          ensure
            Thread.current[:SideJob] = nil
            val = SideJob.redis.multi do |multi|
              multi.get lock
              multi.del lock
            end[0]

            worker.run if val && val.to_f != now # run it again if the lock key changed
            worker.parent.run if worker.parent
          end
        end
      end
    end

    private

    def set_status(worker, status)
      SideJob.redis.hset worker.redis_key, 'status', status
      worker.log 'status', {status: status}
    end

    def log_exception(worker, exception)
      # only store the backtrace until the first sidekiq line
      worker.log 'error', {error: exception.message, backtrace: exception.backtrace.take_while {|l| l !~ /sidekiq/}.join("\n")}
    end
  end
end
