require 'sidekiq'
require 'sidekiq/api'
require 'sidejob/port'
require 'sidejob/job'
require 'sidejob/worker'
require 'sidejob/server_middleware'
require 'time' # for iso8601 method

module SideJob
  # Returns redis connection
  # If block is given, yields the redis connection
  # Otherwise, just returns the redis connection
  def self.redis
    Sidekiq.redis do |redis|
      if block_given?
        yield redis
      else
        redis
      end
    end
  end

  # @param redis [Hash] Options for passing to Redis.new
  def self.redis=(redis)
    Sidekiq.redis = redis
  end

  # Main function to queue a job
  # @param queue [String] Name of the queue to put the job in
  # @param klass [String] Name of the class that will handle the job
  # @param config [Hash] Static job configuration
  # @param parent [SideJob::Job] parent job
  # @param at [Time, Float] Time to schedule the job, otherwise queue immediately
  # @param by [String] Who created this job. Recommend <type>:<id> format for non-jobs as SideJob uses job:<jid>
  # @return [SideJob::Job] Job
  def self.queue(queue, klass, parent: nil, at: nil, by: nil, config: {})
    raise "No worker registered for #{klass} in queue #{queue}" unless SideJob::Worker.config(queue, klass)

    # To prevent race conditions, we generate the jid and set all metadata before queuing the job to sidekiq
    # Otherwise, sidekiq may start the job too quickly
    jid = SideJob.redis.incr(:job_id).to_s
    job = SideJob::Job.new(jid, by: by)

    if parent
      ancestry = [parent.jid] + SideJob.redis.lrange("#{parent.redis_key}:ancestors", 0, -1)
    end

    SideJob.redis.multi do |multi|
      multi.sadd 'jobs', jid
      multi.hmset job.redis_key, 'queue', queue, 'class', klass, 'config', config.to_json,
                  'created_by', by, 'created_at', SideJob.timestamp

      if parent
        multi.rpush "#{job.redis_key}:ancestors", ancestry # we need to rpush to get the right order
        multi.sadd "#{parent.redis_key}:children", jid
      end
    end

    job.run(at: at)
  end

  # Finds a job by id
  # @param job_id [String, nil] Job Id
  # @param by [String] By string to store for associating entities to events
  # @return [SideJob::Job, nil] Job object or nil if it doesn't exist
  def self.find(job_id, by: nil)
    return nil unless job_id
    job = SideJob::Job.new(job_id, by: by)
    return job.exists? ? job : nil
  end

  # Returns the current timestamp as a iso8601 string
  # @return [String] Current timestamp
  def self.timestamp
    Time.now.utc.iso8601(9)
  end
end

# :nocov:
Sidekiq.configure_server do |config|
  config.server_middleware do |chain|
    chain.remove Sidekiq::Middleware::Server::RetryJobs # we never want sidekiq to retry jobs
    chain.add SideJob::ServerMiddleware
  end
end

if ENV['SIDEJOB_URL']
  SideJob.redis = {url: ENV['SIDEJOB_URL']}
end
# :nocov:
