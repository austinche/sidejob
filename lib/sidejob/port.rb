module SideJob
  # Represents an input or output port from a Job
  class Port
    attr_reader :job, :type, :name

    # @param job [SideJob::Job, SideJob::Worker]
    # @param type [:in, :out] Specifies whether it is input or output port
    # @param name [String] Port names should match [a-zA-Z0-9_]+
    def initialize(job, type, name)
      @job = job
      @type = type
      @name = name
      raise "Invalid port name: #{@name}" if @name !~ /^[a-zA-Z0-9_]+$/
    end

    def ==(other)
      other.is_a?(Port) && job == other.job && type == other.type && name.to_s == other.name.to_s
    end

    def eql?(other)
      return self == other
    end

    # Returns the number of items waiting on this port
    # @return [Fixnum]
    def size
      SideJob.redis do |conn|
        conn.llen redis_key
      end
    end

    # Push some data on to the port
    # Records the port so that it can be retrieved by Port.all
    # @param data [Array<String>] List of data to push on to port
    def push(*data)
      Sidekiq::Logging.logger.debug "-> #{to_s} #{data.inspect}"

      SideJob.redis do |conn|
        conn.lpush redis_key, data
      end
      remember
      self
    end

    # Pop data from a port
    # @return [String, nil] First data from port or nil if no data exists
    def pop
      data = SideJob.redis do |conn|
        conn.rpop redis_key
      end

      if data
        Sidekiq::Logging.logger.debug "#{to_s} -> #{data.inspect}"
      end

      data
    end

    # Pops all data from this port and pushes all data to another port
    # @param dst_port [SideJob::Port] Destination port
    # @return [Array<String>] All data moved
    def pop_all_to(dst_port)
      data = []
      SideJob.redis do |conn|
        pushed = false
        loop do
          x = conn.rpoplpush(redis_key, dst_port.redis_key)
          break unless x
          data << x
          pushed = true
        end
        if pushed
          dst_port.remember
          Sidekiq::Logging.logger.debug "#{to_s} -> #{dst_port.to_s} #{data.inspect}"
        end
      end
      data
    end

    # Peek at the next data to be popped from a port
    # @return [String, nil] Data from port or nil if no data exists
    def peek
      SideJob.redis do |conn|
        conn.lrange(redis_key, -1, -1)[0]
      end
    end

    # Removes (pops) the oldest items such that the size is at most the given size
    # @param size [Fixnum] Maximum data to leave on the port
    def trim(size)
      SideJob.redis do |conn|
        conn.ltrim redis_key, 0, size-1
      end
      self
    end

    # Empties the port
    def clear
      SideJob.redis do |conn|
        conn.del redis_key
      end
      self
    end

    # Returns the redis key used for storing inputs or outputs from a port name
    # @return [String] Redis key
    def redis_key
      "#{@job.jid}:#{@type}:#{@name}"
    end

    def hash
      redis_key.hash
    end

    def to_s
      redis_key
    end

    # Have this port be returned by Port.all
    def remember
      SideJob.redis do |conn|
        conn.sadd "#{@job.jid}:#{@type}ports", @name # set to store all port names
      end
    end

    # Returns all ports that have had remember called on it for the given job and type
    # @param job [SideJob::Job, SideJob::Worker]
    # @param type [:in, :out] Specifies whether it is input or output port
    # @return [Array<SideJob::Port>] All pushed to ports for the given job and type
    def self.all(job, type)
      SideJob.redis do |conn|
        conn.smembers("#{job.jid}:#{type}ports").map {|name| SideJob::Port.new(job, type, name)}
      end
    end

    # Deletes all Redis keys for all ports of the given job/type
    # @param job [SideJob::Job, SideJob::Worker]
    # @param type [:in, :out] Specifies whether it is input or output port
    def self.delete_all(job, type)
      SideJob.redis do |conn|
        conn.del ["#{job.jid}:#{type}ports"] +
                     conn.smembers("#{job.jid}:#{type}ports").map {|name| "#{job.jid}:#{type}:#{name}"}
      end
    end
  end
end
