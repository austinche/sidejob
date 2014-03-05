require 'sidekiq'
require 'sidejob/port'
require 'sidejob/job'
require 'sidejob/worker'
require 'sidejob/client_middleware'

Sidekiq.configure_client do |config|
  config.redis = { namespace: 'sidejob' }
  config.client_middleware do |chain|
    chain.add SideJob::ClientMiddleware
  end
end

module SideJob
  # Returns redis connection
  def self.redis(&block)
    Sidekiq.redis do |conn|
      yield conn
    end
  end

  # Main function to queue a job
  # @param queue [String] Name of the queue to put the job in
  # @param klass [String] Name of the class that will handle the job
  # @param args [Array] Arguments to be sent to the class' perform method (must be JSON-serializable)
  # @param parent [SideJob::Job, nil] Parent job or nil if none
  # @return [SideJob::Job] Job
  def self.queue(queue, klass, args=[], parent=nil)
    job_id = Sidekiq::Client.push('queue' => queue, 'class' => klass, 'args' => args, 'retry' => false)
    job = SideJob::Job.new(job_id)
    if parent
      job.set(:parent, parent.jid)
      redis do |conn|
        conn.sadd "#{parent.jid}:children", job_id
      end
    end
    job
  end
end
