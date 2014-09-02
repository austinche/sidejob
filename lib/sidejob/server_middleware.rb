module SideJob
  class ServerMiddleware
    MAX_CALLS_PER_SECOND = 10

    def call(worker, msg, queue)
      # limit each job to being called too many times per second
      # this is to help prevent bad coding that leads to recursive busy loops
      # Uses Rate limiter 1 pattern from http://redis.io/commands/INCR
      key = "rate:#{worker.jid}:#{Time.now.to_i}"
      rate = SideJob.redis.get(key)
      if worker.status == :stopped
      elsif rate && rate.to_i > MAX_CALLS_PER_SECOND
        worker.status = :stopped
        worker.log 'error', {error: "Job was stopped due to being called too rapidly"}
      else
        worker.status = :running
        SideJob.redis.multi do |multi|
          multi.hdel worker.redis_key, :restart
          multi.incr key
          multi.expire key, 10
        end
        Thread.current[:SideJob] = worker
        yield
        worker.status = :completed if worker.status == :running
      end

    rescue => e
      worker.status = :failed
      # only store the backtrace until the first sidekiq line
      worker.log 'error', {error: e.message, backtrace: e.backtrace.take_while {|l| l !~ /sidekiq/}.join("\n")}
    ensure
      Thread.current[:SideJob] = nil
      time = SideJob.redis.hget worker.redis_key, :restart
      worker.restart(time.to_f) if time

      worker.parent.restart if worker.parent
    end
  end
end
