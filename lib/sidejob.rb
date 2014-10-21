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
  # @param args [Array] additional args to pass to the worker's perform method (default none)
  # @param parent [SideJob::Job] parent job
  # @param at [Time, Float] Time to schedule the job, otherwise queue immediately
  # @param by [String] Who created this job. Recommend <type>:<id> format for non-jobs as SideJob uses job:<id>
  # @param inports [Hash{Symbol,String => Hash}] Input port configuration. Port name to options.
  # @param outports [Hash{Symbol,String => Hash}] Output port configuration. Port name to options.
  # @return [SideJob::Job] Job
  def self.queue(queue, klass, args: nil, parent: nil, at: nil, by: nil, inports: nil, outports: nil)
    config = SideJob::Worker.config(queue, klass)
    raise "No worker registered for #{klass} in queue #{queue}" unless config

    # To prevent race conditions, we generate the id and set all data in redis before queuing the job to sidekiq
    # Otherwise, sidekiq may start the job too quickly
    id = SideJob.redis.incr(:job_id).to_s
    job = SideJob::Job.new(id, by: by)

    if parent
      ancestry = [parent.id] + SideJob.redis.lrange("#{parent.redis_key}:ancestors", 0, -1)
    end

    inports = (inports || {}).stringify_keys
    outports = (outports || {}).stringify_keys
    inports.each_key {|port| inports[port] = inports[port].stringify_keys }
    outports.each_key {|port| outports[port] = outports[port].stringify_keys }
    ports = {
        in: (config['inports'] || {}).merge(inports),
        out: (config['outports'] || {}).merge(outports || {}),
    }

    SideJob.redis.multi do |multi|
      multi.hset 'job', id, {queue: queue, class: klass, args: args, created_by: by, created_at: SideJob.timestamp}.to_json

      ports.each_key do |type|
        modes = ports[type].map do |port, options|
          [port, options['mode'] || 'queue']
        end.flatten(1)
        multi.hmset "job:#{id}:#{type}ports:mode", *modes if modes.length > 0

        defaults = ports[type].map do |port, options|
          if options.has_key?('default')
            [port, options['default'].to_json]
          else
            nil
          end
        end.compact.flatten(1)
        multi.hmset "job:#{id}:#{type}ports:default", *defaults if defaults.length > 0
      end

      if parent
        multi.rpush "#{job.redis_key}:ancestors", ancestry # we need to rpush to get the right order
        multi.sadd "#{parent.redis_key}:children", id
      end
    end

    # send initial data to ports
    ports.each_key do |type|
      ports[type].each_pair do |port, options|
        if options['data']
          options['data'].each do |x|
            job.input(port).write x
          end
        end
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
