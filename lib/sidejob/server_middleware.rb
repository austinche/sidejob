module SideJob
  class ServerMiddleware
    def call(worker, msg, queue)
      worker.status = :running
      SideJob.redis.hdel worker.redis_key, :restart
      yield
    rescue => e
      worker.status = :failed
      worker.log 'error', {error: e.message, backtrace: e.backtrace}
      raise e
    ensure
      worker.status = :completed if worker.status == :running

      time = SideJob.redis.hget worker.redis_key, :restart
      worker.restart(time.to_f) if time

      worker.parent.restart if worker.parent
    end
  end
end
