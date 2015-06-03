# helpers for testing

module SideJob
  module Worker
    # Run jobs until the queue is cleared.
    # @param timeout [Float] timeout in seconds for Timeout#timeout (default 5)
    # @param errors [Boolean] Whether to propagate errors that occur in jobs (default true)
    def self.drain_queue(timeout: 5, errors: true)
      Timeout::timeout(timeout) do
        have_job = true
        while have_job
          have_job = false
          Sidekiq::Queue.all.each do |queue|
            queue.each do |sidekiq_job|
              have_job = true
              sidekiq_job.delete

              SideJob.find(sidekiq_job.jid).run_inline(errors: errors, queue: false, args: sidekiq_job.args)
            end
          end
        end
      end
    end
  end

  class Job
    # Runs a single job once. This method only works for jobs with no child jobs.
    # @param errors [Boolean] Whether to propagate errors that occur in jobs (default true)
    # @param queue [Boolean] Whether to force the job to be queued (default true)
    # @param args [Array] Args to pass to the worker's perform method (default none)
    def run_inline(errors: true, queue: true, args: [])
      self.status = 'queued' if queue

      worker = get(:class).constantize.new
      worker.jid = id
      SideJob::ServerMiddleware.raise_errors = errors
      SideJob::ServerMiddleware.new.call(worker, {}, get(:queue)) do
        worker.perform(*args)
      end
    ensure
      SideJob::ServerMiddleware.raise_errors = false
    end
  end
end
