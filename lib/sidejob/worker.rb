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

    # Finds a job by id, setting by string to job:<id>
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
    # of dealing with ports with defaults.
    # A worker should be idempotent (it can be called multiple times on the same state).
    # Consider reading from a single port with a default value. Each time it is run, it could read the same data
    # from the port. The output of the job then could depend on the number of times it is run.
    # To prevent this, this method requires that there be at least one input port which does not have a default.
    # Yields data from the ports until either no ports have data or is suspended due to data on some but not all ports.
    # @param inputs [Array<String>] List of input ports to read
    # @yield [Array] Splat of input data in same order as inputs
    # @raise [SideJob::Worker::Suspended] Raised if an input port without a default has data but not all ports
    # @raise [RuntimeError] An error is raised if all input ports have default values
    def for_inputs(*inputs, &block)
      return unless inputs.length > 0
      ports = inputs.map {|name| input(name)}
      loop do
        # error if ports all have defaults, complete if no non-default port inputs, suspend if partial inputs
        data = ports.map {|port| [ port.data?, port.default? ] }
        raise "One of these input ports should not have a default value: #{inputs.join(',')}" if data.all? {|x| x[1]}
        return unless data.any? {|x| x[0] && ! x[1] }
        suspend unless data.all? {|x| x[0] }

        yield *ports.map(&:read)
      end
    end

    # Sets values in the job's internal state.
    # @param data [Hash{String,Symbol => Object}] Data to update: objects should be JSON encodable
    # @raise [RuntimeError] Error raised if job no longer exists
    def set(data)
      return unless data.size > 0
      load_state
      data.each_pair { |key, val| @state[key.to_s] = val }
      save_state
    end

    # Unsets some fields in the job's internal state
    # @param fields [Array<String,Symbol>] Fields to unset
    # @raise [RuntimeError] Error raised if job no longer exists
    def unset(*fields)
      return unless fields.length > 0
      load_state
      fields.each { |field| @state.delete(field.to_s) }
      save_state
    end

    private

    def by_string
      "job:#{@id}"
    end

    def save_state
      check_exists
      if @state
        SideJob.redis.hset 'job', @id, @state.to_json
      end
    end
  end
end
