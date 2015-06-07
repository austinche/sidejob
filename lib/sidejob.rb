require 'sidekiq'
require 'sidekiq/api'
require 'sidejob/version'
require 'sidejob/port'
require 'sidejob/job'
require 'sidejob/worker'
require 'sidejob/server_middleware'
require 'time' # for iso8601 method
require 'securerandom'
require 'pathname'

module SideJob
  # Configuration parameters
  CONFIGURATION = {
      lock_expiration: 60, # workers should not run longer than this number of seconds
      max_runs_per_minute: 600, # terminate jobs that run too often
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

    # To prevent race conditions, we generate the id and set all data in redis before queuing the job to sidekiq
    # Otherwise, sidekiq may start the job too quickly
    id = SideJob.redis.incr('jobs:last_id')
    SideJob.redis.sadd 'jobs', id
    job = SideJob::Job.new(id)

    redis_key = job.redis_key
    SideJob.redis.multi do |multi|
      multi.set "#{redis_key}:worker", {queue: queue, class: klass, args: args}.to_json
      multi.set "#{redis_key}:status", 'completed'
      multi.set "#{redis_key}:created_at", SideJob.timestamp
      multi.set "#{redis_key}:created_by", by
    end

    if parent
      raise 'Missing name option for job with a parent' unless name
      parent.adopt(job, name)
    end

    # initialize ports
    job.inports = inports
    job.outports = outports

    job.run(at: at)
  end

  # Finds a job by name or id.
  # @param name_or_id [String, Integer] Job name or id
  # @return [SideJob::Job, nil] Job object or nil if it doesn't exist
  def self.find(name_or_id)
    SideJob::Job.new(name_or_id) rescue nil
  end

  # Returns the current timestamp as a iso8601 string
  # @return [String] Current timestamp
  def self.timestamp
    Time.now.utc.iso8601(9)
  end

  # Publishes a log message using the current SideJob context.
  # @param entry [Hash] Log entry
  def self.log(entry)
    context = (Thread.current[:sidejob_context] || {}).merge(timestamp: SideJob.timestamp)
    SideJob.publish '/sidejob/log', context.merge(entry)
  end

  # Adds to the current SideJob context within the block.
  # @param data [Hash] Data to be merged into the current context
  def self.context(data, &block)
    previous = Thread.current[:sidejob_context]
    Thread.current[:sidejob_context] = (previous || {}).merge(data.symbolize_keys)
    yield
  ensure
    Thread.current[:sidejob_context] = previous
  end

  # Publishes a message up the channel hierarchy to jobs by writing to ports subscribed to the channel.
  # Also publishes to the destination channel only via normal redis pubsub.
  # @param channel [String] Channel is path-like, separated by / to indicate hierarchy
  # @param message [Object] JSON encodable message
  def self.publish(channel, message)
    # We don't publish at every level up hierarchy via redis pubsub since a client can use redis psubscribe
    SideJob.redis.publish channel, message.to_json

    job_subs = {}

    # walk up the channel hierarchy
    Pathname.new(channel).ascend do |channel|
      channel = channel.to_s
      jobs = SideJob.redis.smembers "channel:#{channel}"
      jobs.each do |id|
        job = SideJob.find(id)
        if ! job_subs.has_key?(id)
          job_subs[id] = {}
          if job
            SideJob.redis.hgetall("#{job.redis_key}:inports:channels").each_pair do |port, channels|
              channels = JSON.parse(channels)
              channels.each do |ch|
                job_subs[id][ch] ||= []
                job_subs[id][ch] << port
              end
            end
          end
        end

        if job && job_subs[id] && job_subs[id][channel]
          job_subs[id][channel].each do |port|
            job.input(port).write message
          end
        else
          # Job is gone or no longer subscribed to this channel
          SideJob.redis.srem "channel:#{channel}", id
        end
      end
    end
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
elsif ENV['SIDEJOB_HOST']
  SideJob.redis = {host: ENV['SIDEJOB_HOST']}
end
# :nocov:
