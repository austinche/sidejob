require 'sidekiq'
require 'sidekiq/api'
require 'sidejob/port'
require 'sidejob/job'
require 'sidejob/worker'
require 'sidejob/server_middleware'
require 'time' # for iso8601 method

module SideJob
  # Configuration parameters
  CONFIGURATION = {
      lock_expiration: 86400, # workers should not run longer than this number of seconds
      max_depth: 20, # jobs should not be nested more than this number of levels
      max_runs_per_minute: 120, # terminate jobs that run too often
  }

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
  # @param args [Array] additional args to pass to the worker's perform method (default none)
  # @param parent [SideJob::Job] parent job
  # @param name [String] Name of child job (required if parent specified)
  # @param at [Time, Float] Time to schedule the job, otherwise queue immediately
  # @param by [String] Who created this job. Recommend <type>:<id> format for non-jobs as SideJob uses job:<id>.
  # @param inports [Hash{Symbol,String => Hash}] Input port configuration. Port name to options.
  # @param outports [Hash{Symbol,String => Hash}] Output port configuration. Port name to options.
  # @return [SideJob::Job] Job
  def self.queue(queue, klass, args: nil, parent: nil, name: nil, at: nil, by: nil, inports: nil, outports: nil)
    raise "No worker registered for #{klass} in queue #{queue}" unless SideJob::Worker.config(queue, klass)

    log_options = {}
    if parent
      raise 'Missing name option for job with a parent' unless name
      raise "Parent already has child job with name #{name}" if parent.child(name)
      ancestry = [parent.id] + SideJob.redis.lrange("#{parent.redis_key}:ancestors", 0, -1)

      # prevent too deep of a job tree which may be a sign of a coding problem
      raise "Job tree depth > #{CONFIGURATION[:max_depth]}" if ancestry.length > CONFIGURATION[:max_depth]

      log_options = {job: parent.id}
    end

    # To prevent race conditions, we generate the id and set all data in redis before queuing the job to sidekiq
    # Otherwise, sidekiq may start the job too quickly
    id = SideJob.redis.incr('jobs:last_id').to_s
    job = SideJob::Job.new(id)

    SideJob.redis.multi do |multi|
      multi.hset 'jobs', id, {queue: queue, class: klass, args: args, created_by: by, created_at: SideJob.timestamp}.to_json

      if parent
        multi.rpush "#{job.redis_key}:ancestors", ancestry # we need to rpush to get the right order
        multi.hset "#{parent.redis_key}:children", name, id
      end
    end

    # initialize ports
    job.inports = inports
    job.outports = outports

    job.run(at: at)
  end

  # Finds a job by id
  # @param job_id [String, nil] Job Id
  # @return [SideJob::Job, nil] Job object or nil if it doesn't exist
  def self.find(job_id)
    return nil unless job_id
    job = SideJob::Job.new(job_id)
    return job.exists? ? job : nil
  end

  # Returns the current timestamp as a iso8601 string
  # @return [String] Current timestamp
  def self.timestamp
    Time.now.utc.iso8601(9)
  end

  # Adds a log entry to redis with current timestamp.
  # @param entry [Hash] Log entry
  def self.log(entry)
    context = (Thread.current[:sidejob_log_context] || {}).merge(timestamp: SideJob.timestamp)
    SideJob.redis.rpush 'jobs:logs', context.merge(entry).to_json
  end

  # Return all job logs and optionally clears them.
  # @param clear [Boolean] If true, delete logs after returning them (default true)
  # @return [Array<Hash>] All logs with the oldest first
  def self.logs(clear: true)
    SideJob.redis.multi do |multi|
      multi.lrange 'jobs:logs', 0, -1
      multi.del 'jobs:logs' if clear
    end[0].map {|log| JSON.parse(log)}
  end

  # Adds the given metadata to all {SideJob.log} calls within the block.
  # @param metadata [Hash] Metadata to be merged with each log entry
  def self.log_context(metadata, &block)
    previous = Thread.current[:sidejob_log_context]
    Thread.current[:sidejob_log_context] = (previous || {}).merge(metadata.symbolize_keys)
    yield
  ensure
    Thread.current[:sidejob_log_context] = previous
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
