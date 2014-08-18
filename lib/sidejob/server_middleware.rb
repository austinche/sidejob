module SideJob
  class ServerMiddleware
    def call(worker, msg, queue)
      worker.status = :running
      SideJob.redis do |redis|
        redis.hdel worker.redis_key, :restart
      end
      yield
      worker.status = :completed
    rescue SideJob::Worker::Suspended
      worker.status = :suspended
    rescue => e
      worker.status = :failed
      worker.log_push 'error', {error: e.message, backtrace: e.backtrace}
    ensure
      worker.restart if worker.restarting?
    end
  end
end
