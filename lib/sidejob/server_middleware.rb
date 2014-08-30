module SideJob
  class ServerMiddleware
    def call(worker, msg, queue)
      worker.status = :running
      SideJob.redis.hdel worker.redis_key, :restart
      yield
      worker.status = :completed if worker.status == :running
    rescue => e
      worker.status = :failed
      # only store the backtrace until the first sidekiq line
      worker.log 'error', {error: e.message, backtrace: e.backtrace.take_while {|l| l !~ /sidekiq/}.join("\n")}
    ensure
      time = SideJob.redis.hget worker.redis_key, :restart
      worker.restart(time.to_f) if time

      worker.parent.restart if worker.parent
    end
  end
end
