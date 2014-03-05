module SideJob
  class ServerMiddleware
    def call(worker, msg, queue)
      worker.status = :working
      yield
      new_status = :completed
    rescue SideJob::Worker::Suspended
      new_status = :suspended
    rescue => e
      new_status = :failed
      worker.set :error, e.message
    ensure
      restart = (worker.status == :restarting)
      worker.status = new_status
      worker.restart if restart
    end
  end
end
