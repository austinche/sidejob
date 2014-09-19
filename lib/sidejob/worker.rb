module SideJob
  # All workers should include SideJob::Worker and implement the perform method.
  # @see SideJob::JobMethods
  module Worker
    # Registry methods are available both on the module SideJob::Worker and from inside Worker classes
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

      # Unregister a worker
      # @param queue [String] Name of queue
      # @param klass [String] Name of worker class
      def unregister(queue, klass)
        SideJob.redis.hdel "workers:#{queue}", klass
      end
    end
    SideJob::Worker.extend(RegistryMethods)

    # Class methods added to Workers
    module ClassMethods
      # Worker specific configuration for how it should be run
      # @see SideJob::ServerMiddleware
      CONFIGURATION_KEYS = %i{log_status lock_expiration max_calls_per_min max_depth}
      attr_reader :configuration

      # Override some runtime parameters for running this worker class
      # @see SideJob::ServerMiddleware
      def configure(options={})
        unknown_keys = (options.keys - CONFIGURATION_KEYS)
        raise "Unknown configuration keys #{unknown_keys.join(',')}" if unknown_keys.any?
        @configuration = options
      end
    end

    # Exception raised by {#suspend}
    class Suspended < StandardError; end

    # @see SideJob::Worker
    def self.included(base)
      base.class_eval do
        include Sidekiq::Worker
        include SideJob::JobMethods
      end
      base.extend(RegistryMethods)
      base.extend(ClassMethods)
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
      data = input(field).drain.last
      if data.nil?
        data = get(field)
      else
        set({field => data})
      end
      data
    end

    private

    def by_string
      "job:#{@jid}"
    end
  end
end
