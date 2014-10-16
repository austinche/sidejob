module SideJob
  # All workers should include SideJob::Worker and implement the perform method.
  # @see SideJob::JobMethods
  module Worker
    @registry ||= {}
    class << self
      # This holds the registry for all available workers on one queue
      attr_reader :registry

      # Workers need to add themselves to the registry even if it's an empty configuration.
      # This method publishes the registry to redis so that other workers can call workers on this queue.
      # All workers for the queue should be defined as the existing registry is overwritten.
      # @param queue [String] Queue to register all defined workers
      def register_all(queue)
        SideJob.redis.multi do |multi|
          multi.del "workers:#{queue}"
          multi.hmset "workers:#{queue}", @registry.map {|key, val| [key, val.to_json]}.flatten(1) if @registry.size > 0
        end
      end

      # Returns the configuration registered for a worker.
      # @param queue [String] Name of queue
      # @param klass [String] Name of worker class
      # @return [Hash, nil] Returns nil if the worker is not defined
      # @see ClassMethods#register
      def config(queue, klass)
        config = SideJob.redis.hget "workers:#{queue}", klass
        config = JSON.parse(config) if config
        config
      end
    end

    # Class methods added to Workers
    module ClassMethods
      # All workers need to register themselves
      # @param config [Hash] The base configuration used by any jobs of this class
      def register(config={})
        SideJob::Worker.registry[self.name] = config
      end
    end

    # @see SideJob::Worker
    def self.included(base)
      base.class_eval do
        include Sidekiq::Worker
        include SideJob::JobMethods
      end
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

    # Exception raised by {#suspend}
    class Suspended < StandardError; end

    # Immediately suspend the current worker
    # @raise [SideJob::Worker::Suspended]
    def suspend
      raise Suspended
    end

    # Reads a set of input ports together.
    # Workers should use this method where possible instead of reading directly from ports due to complexities
    # of dealing with memory ports. A worker should be idempotent (it can be called multiple times on the same state).
    # Consider a job with a single memory port. Each time it is run, it could read the same data from the port.
    # The output of the job then could depend on the number of times it is run. To prevent this, this method
    # requires that there be at least one non-memory input port.
    # Yields data from the ports until no non-memory ports have data or is suspended due to missing data.
    # @param inputs [Array<String>] List of input ports to read
    # @yield [Array] Splat of input data in same order as inputs
    # @raise [SideJob::Worker::Suspended] Raised if some non-memory input port has data but not all
    def for_inputs(*inputs, &block)
      ports = inputs.map {|name| input(name)}
      loop do
        # complete if no non-memory port inputs, suspend if partial inputs
        data = ports.map {|port| [ port.data?, port.infinite? ] }
        return unless data.any? {|x| x[0] && ! x[1] }
        suspend unless data.all? {|x| x[0] }

        yield *ports.map(&:read)
      end
    end

    private

    def by_string
      "job:#{@jid}"
    end
  end
end
