module SideJob
  # All workers should include SideJob::Worker and implement the perform method.
  # @see SideJob::JobMethods
  module Worker
    class Suspended < StandardError
    end

    module ClassMethods
    end

    def self.included(base)
      base.class_eval do
        include Sidekiq::Worker
        include SideJob::JobMethods
      end
      base.extend(ClassMethods)
    end

    # Helper to store current progress
    def at(num, total)
      mset({ num: num, total: total })
    end

    # Suspend the current worker
    # Will restart upon manual call to SideJob.restart or when a child job changes status
    # @raise [SideJob::Worker::Suspended]
    def suspend
      raise Suspended
    end
  end
end
