require 'sidekiq'
require 'sidekiq/api'
require 'sidejob/port'
require 'sidejob/job'
require 'sidejob/worker'
require 'sidejob/client_middleware'
require 'sidejob/server_middleware'
require 'sidejob/graph'

Sidekiq.configure_client do |config|
  config.redis = { namespace: 'sidejob' }
  config.client_middleware do |chain|
    chain.add SideJob::ClientMiddleware
  end
end

Sidekiq.configure_server do |config|
  config.redis = { namespace: 'sidejob' }
  config.server_middleware do |chain|
    chain.add SideJob::ServerMiddleware
  end
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
  # @param options [Hash] Additional options, keys should be symbols
  #   args: [Array] additional args to pass to the class (default none)
  # @return [SideJob::Job] Job
  def self.queue(queue, klass, options={})
    args = options[:args] || []
    job_id = Sidekiq::Client.push('queue' => queue, 'class' => klass, 'args' => args, 'retry' => false)
    SideJob::Job.new(job_id)
  end

  # Finds a job by id
  # @param job_id [String] Job Id
  # @return [SideJob::Job, nil] Job object or nil if it doesn't exist
  def self.find(job_id)
    exists = redis { |conn| conn.exists job_id }
    if exists
      SideJob::Job.new(job_id)
    else
      nil
    end
  end
end
