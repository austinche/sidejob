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

    # Methods loaded last to override other included methods
    module OverrideMethods
      # Returns the jid set by sidekiq as the job id
      # @return [Integer] Job id
      def id
        jid.to_i
      end
    end

    # @see SideJob::Worker
    def self.included(base)
      base.class_eval do
        include Sidekiq::Worker
        include SideJob::JobMethods
        include SideJob::Worker::OverrideMethods
      end
      base.extend(ClassMethods)
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
    # If all input port have defaults, this method remembers the call and will only yield once even over multiple runs.
    # In addition, any writes to output ports inside the block will instead set the default value of the port.
    # Yields data from the ports until either no ports have data or is suspended due to data on some but not all ports.
    # @param inputs [Array<String>] List of input ports to read
    # @yield [Array] Splat of input data in same order as inputs
    # @raise [SideJob::Worker::Suspended] Raised if an input port without a default has data but not all ports
    def for_inputs(*inputs, &block)
      return unless inputs.length > 0
      ports = inputs.map {|name| input(name)}
      loop do
        SideJob::Port.log_group do
          info = ports.map {|port| [ port.size > 0, port.default? ] }

          return unless info.any? {|x| x[0] || x[1]} # Nothing to do if there's no data to read
          if info.any? {|x| x[0]}
            # some port has data, suspend unless every port has data or default
            suspend unless info.all? {|x| x[0] || x[1] }
            yield *ports.map(&:read)
          elsif info.all? {|x| x[1]}
            # all ports have default and no data
            defaults = ports.map(&:default)
            last_default = get(:for_inputs) || []
            return unless defaults != last_default
            set({for_inputs: defaults})
            begin
              Thread.current[:sidejob_port_write_default] = true
              yield *defaults
            ensure
              Thread.current[:sidejob_port_write_default] = nil
            end
            return
          else
            # No ports have data and not every port has a default value so nothing to do
            return
          end
        end
      end
    end
  end
end
