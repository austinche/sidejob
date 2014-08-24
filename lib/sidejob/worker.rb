module SideJob
  # All workers should include SideJob::Worker and implement the perform method.
  # @see SideJob::JobMethods
  module Worker
    module ClassMethods
    end

    def self.included(base)
      base.class_eval do
        include Sidekiq::Worker
        include SideJob::JobMethods
      end
      base.extend(ClassMethods)
    end

    # Suspend the current worker
    # Will restart upon call to SideJob::Job#restart
    def suspend
      self.status = :suspended
    end
  end
end
