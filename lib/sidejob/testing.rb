# helpers for testing

module SideJob
  module Worker
    # Drain all queued jobs
    # @param timeout [Float] timeout in seconds for Timeout#timeout (default 5)
    # @param raise_on_errors [Boolean] Whether to re-raise errors that occur in jobs (default true)
    def self.drain_queue(timeout: 5, raise_on_errors: true)
      Timeout::timeout(timeout) do
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

              if raise_on_errors && worker.status == 'failed'
                error = worker.logs.detect {|log| log['type'] == 'error'}
                if error
                  exception = RuntimeError.exception(error['error'])
                  exception.set_backtrace(error['backtrace'])
                  raise exception
                else
                  raise "Job #{job.klass} failed but cannot find error log"
                end
              end
            end
          end
        end
      end
    end
  end
end
