module SideJob
  class ClientMiddleware
    def call(worker_class, msg, queue)
      # we store original call so we can restart
      Sidekiq.redis {|conn| conn.hset(msg['jid'], 'call', JSON.generate(msg))}
      SideJob::Job.new(msg['jid']).status = :queued
      yield
    end
  end
end
