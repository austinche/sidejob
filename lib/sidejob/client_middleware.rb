module SideJob
  class ClientMiddleware
    def call(worker_class, msg, queue, redis_pool)
      job = SideJob::Job.new(msg['jid'])
      job.set_json :call, msg      # we store original call so we can restart
      job.status = :queued
      yield
    end
  end
end
