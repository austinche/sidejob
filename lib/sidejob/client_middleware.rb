module SideJob
  class ClientMiddleware
    def call(worker_class, msg, queue, redis_pool)
      job = SideJob::Job.new(msg['jid'])
      SideJob.redis do |redis|
        redis.hset job.redis_key, :call, JSON.generate(msg)      # we store original call so we can restart in SideJob::Job#restart
      end
      job.status = :queued
      yield
    end
  end
end
