# helpers for testing

module SideJob
  module Worker
    # Drain all queued jobs
    def self.drain_queue
      have_job = true
      while have_job
        have_job = false
        Sidekiq::Queue.all.each do |queue|
          queue.each do |job|
            have_job = true
            job.delete

            worker = job.klass.constantize.new
            worker.jid = job.jid
            SideJob::ServerMiddleware.new.call(worker, job, job.queue) do
              worker.perform(*job.args)
            end
          end
        end
      end
    end
  end
end
