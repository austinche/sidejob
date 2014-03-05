# helpers for testing

require 'sidekiq/testing'

module SideJob
  module Worker
    module ClassMethods
      # Overwrite Sidekiq::Worker drain method to use our middleware also
      def drain
        while job = jobs.shift do
          worker = new
          worker.jid = job['jid']
          SideJob::ServerMiddleware.new.call(worker, job, job['queue']) do
            worker.perform(*job['args'])
          end
        end
      end
    end
  end
end
