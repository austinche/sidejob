module SideJob
  # All workers should include SideJob::Worker and implement the perform method.
  # @see SideJob::JobMethods
  module Worker
    @all = []
    class << self
      attr_reader :all
      def register(spec)
        @all << spec
      end
    end

    class Suspended < StandardError
    end

    module ClassMethods
      # Registers a worker class
      # The spec is unused by sidejob and can be in any format
      # Use SideJob::Worker.all to return an array of registered worker specs
      # @param spec [Hash]
      def register(spec)
        SideJob::Worker.register(spec)
      end
    end

    def self.included(base)
      base.class_eval do
        include Sidekiq::Worker
        include SideJob::JobMethods
      end
      base.extend(ClassMethods)
    end

    # Suspend the current worker
    # Will restart upon manual call to SideJob.restart or when a child job changes status
    # @raise [SideJob::Worker::Suspended]
    def suspend
      raise Suspended
    end
  end
end
