module SideJob
  # All workers should include SideJob::Worker and implement the perform method.
  # @see SideJob::JobMethods
  module Worker
    # have these methods be available both on the module SideJob::Worker and from inside Worker classes
    module RegistryMethods
      # Provide a simple way to store worker info
      # @param queue [String] Name of queue
      # @param klass [String] Name of worker class
      # @param spec [Hash] This spec is unused by SideJob so can be in any client format
      def register(queue, klass, spec)
        SideJob.redis.hset "workers:#{queue}", klass, JSON.generate(spec)
      end

      # Returns spec registered with register
      # @param queue [String] Name of queue
      # @param klass [String] Name of worker class
      # @return [Hash, nil]
      def spec(queue, klass)
        spec = SideJob.redis.hget "workers:#{queue}", klass
        spec = JSON.parse(spec) if spec
      end
    end
    SideJob::Worker.extend(RegistryMethods)

    class Suspended < StandardError; end

    def self.included(base)
      base.class_eval do
        include Sidekiq::Worker
        include SideJob::JobMethods
      end
      base.extend(RegistryMethods)
    end

    # Queues a child job
    # @see SideJob.queue
    def queue(queue, klass, **options)
      SideJob.queue(queue, klass, options.merge({parent: self, by: by_string}))
    end

    # Finds a job by id, setting by string to job:<jid>
    # @see SideJob.find
    def find(job_id)
      SideJob.find(job_id, by: by_string)
    end

    # Immediately suspend the current worker
    # @raise [SideJob::Worker::Suspended]
    def suspend
      raise Suspended
    end

    # Helps with getting and storing configuration-like data from a port
    # The assumption is that a configuration port only cares about the last data received on it
    # The last data is also saved in to the state
    # If no data in on the input port, load from saved state
    # @param field [String,Symbol] Name of configuration field/port
    # @return [String, nil] Configuration value or nil
    def get_config(field)
      data = input(field).drain.first
      if data
        set(field, data)
      else
        data = get(field)
      end
      data
    end

    # @see #get_config
    # @param field [String,Symbol] Field to get
    # @return [Object, nil] JSON parsed value of the given configuration value
    def get_config_json(field)
      data = get_config(field)
      if data
        JSON.parse(data)
      else
        nil
      end
    end

    private

    def by_string
      "job:#{@jid}"
    end
  end
end
