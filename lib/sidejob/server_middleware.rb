module SideJob
  class ServerMiddleware
    def call(worker, msg, queue)
      worker.status = :running
      SideJob.redis do |redis|
        redis.hdel worker.redis_key, :restart
      end
      yield
    rescue => e
      worker.status = :failed
      worker.log_push 'error', {error: e.message, backtrace: e.backtrace}
      raise e
    ensure
      worker.status = :completed if worker.status == :running

      time = SideJob.redis do |redis|
        redis.hget worker.redis_key, :restart
      end
      worker.restart(time.to_f) if time

      worker.parent.restart if worker.parent
    end
  end
end
