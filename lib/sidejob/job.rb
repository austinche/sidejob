module SideJob
  # Methods shared between SideJob::Job and SideJob::Worker
  module JobMethods
    attr_reader :jid

    def ==(other)
      other.respond_to?(:jid) && jid == other.jid
    end

    def eql?(other)
      self == other
    end

    def hash
      jid.hash
    end

    # Queues a child job
    # @see SideJob.queue
    def queue(queue, klass, args=[])
      @children = nil
      SideJob.queue(queue, klass, args, self)
    end

    # Sets multiple values
    # Merges data into a job's metadata
    # Updates updated_at field with current timestamp
    # @param data [Hash{String => String}] Data to update
    def mset(data)
      SideJob.redis do |conn|
        conn.hmset jid, 'updated_at', Time.now.to_i, *(data.to_a.flatten(1))
      end
    end

    # Sets a single data in the job's metadata
    # @param field [String,Symbol] Field to set
    # @param value [String]
    # @return [String] Value of the given data field
    def set(field, value)
      mset({field => value})
    end

    # Loads data from the job's metadata
    # @param fields [Array<String,Symbol>] Fields to load or all fields if none specified
    # @return [Hash{String,Symbol => String}] Job's metadata with the fields specified
    def mget(*fields)
      SideJob.redis do |conn|
        if fields.length > 0
          values = conn.hmget(jid, *fields)
          Hash[fields.zip(values)]
        else
          conn.hgetall jid
        end
      end
    end

    # Gets a single data from the job's metadata
    # @param field [String,Symbol] Field to get
    # @return [String] Value of the given data field
    def get(field)
      mget(field)[field]
    end

    # Retrieve the job's status
    # @return [Symbol] Job status
    def status
      st = get(:status)
      return st ? st.to_sym : nil
    end

    # Set the job's status
    # If status is set to anything other than queued, any parent job is restarted
    # @param status [String, Symbol] New status
    def status=(status)
      set(:status, status)

      if status.to_sym != :queued
        # see if a parent job is specified
        parent.restart if parent
      end
    end

    # @return [Array<String>] List of children job ids for the given job
    def children
      @children ||= SideJob.redis do |conn|
        conn.smembers("#{jid}:children").map {|id| SideJob::Job.new(id)}
      end
    end

    # @return [SideJob::Job, nil] Parent job or nil if none
    def parent
      return @parent if @parent # parent job will never change
      @parent = get(:parent)
      @parent = SideJob::Job.new(@parent) if @parent
      return @parent
    end

    # Restart the job
    # If the job status is not running (:completed, :suspended, :failed), queues it immediately
    # If the job status is :queued or :restarting, does nothing
    # If the job status is :working, sets status to :restarting and the job will be restarted by SideJob::ServerMiddleware
    def restart
      case status
        when :queued, :restarting
          # nothing needs to be done
        when :working
          status = :restarting
        when :completed, :suspended, :failed
          original_message = get(:call)
          if original_message
            Sidekiq::Client.push(JSON.load(original_message))
          end
      end
    end

    # Deletes and unschedules the job and all children jobs (recursively)
    def delete
      # recursively delete all children first
      children.each do |child|
        child.delete
      end

      # remove from sidekiq queue
      Sidekiq::Queue.all.each do |queue|
        queue.each do |job|
          job.delete if job.jid == jid
        end
      end

      # delete all SideJob keys
      SideJob::Port.delete_all(self, :in)
      SideJob::Port.delete_all(self, :out)
      SideJob.redis do |conn|
        conn.del [jid, "#{jid}:children"]
      end
    end

    # Returns an input port
    # @param port [String] Name of the port
    # @return [SideJob::Port]
    def input(port)
      SideJob::Port.new(self, :in, port)
    end

    # Returns an output port
    # @param port [String] Name of the port
    # @return [SideJob::Port]
    def output(port)
      SideJob::Port.new(self, :out, port)
    end

    # Gets all input ports that have been pushed to
    # @return [Array<SideJob::Port>] Input ports
    def inports
      SideJob::Port.all(self, :in)
    end

    # Gets all output ports that have been pushed to
    # @return [Array<SideJob::Port>] Output ports
    def outports
      SideJob::Port.all(self, :out)
    end
  end

  # Wrapper for a job which may not be in progress unlike SideJob::Worker
  # @see SideJob::JobMethods
  class Job
    include JobMethods

    # @param jid [String] Job id
    def initialize(jid)
      @jid = jid
    end

    def to_s
      @jid
    end
  end
end
