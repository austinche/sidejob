require 'sidekiq'
require 'sidekiq/api'
require 'sidejob/port'
require 'sidejob/job'
require 'sidejob/worker'
require 'sidejob/server_middleware'
require 'time' # for iso8601 method

Sidekiq.configure_server do |config|
  config.server_middleware do |chain|
    chain.add SideJob::ServerMiddleware
  end
end

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

  # Main function to queue a job
  # @param queue [String] Name of the queue to put the job in
  # @param klass [String] Name of the class that will handle the job
  # @param options [Hash] Additional options, keys should be symbols
  #   parent: [SideJob::Job] parent job
  #   args: [Array] additional args to pass to the class (default none)
  #   at: [Float] Time to schedule the job, otherwise queue immediately
  # @return [SideJob::Job] Job
  def self.queue(queue, klass, options={})
    args = options[:args] || []

    # To prevent race conditions, we generate the jid and set all metadata before queuing the job to sidekiq
    # Otherwise, sidekiq may start the job too quickly
    jid = SideJob.redis.incr(:job_id).to_s
    job = SideJob::Job.new(jid)

    top = options[:parent] ? options[:parent].top.jid : jid

    SideJob.redis.multi do |multi|
      multi.sadd 'jobs', jid
      multi.hmset job.redis_key, 'status', :starting, 'queue', queue, 'class', klass,
                  'args', JSON.generate(args), 'top', top, 'created_at', Time.now.utc.iso8601

      if options[:parent]
        multi.hset job.redis_key, 'parent', options[:parent].jid
        multi.sadd "#{options[:parent].redis_key}:children", jid
      end
    end

    # Now actually queue the job
    job.restart options[:at]

    job
  end

  # Finds a job by id
  # @param job_id [String] Job Id
  # @return [SideJob::Job, nil] Job object or nil if it doesn't exist
  def self.find(job_id)
    job = SideJob::Job.new(job_id)
    return job.exists? ? job : nil
  end
end
