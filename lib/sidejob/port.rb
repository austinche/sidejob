module SideJob
  # Represents an input or output port from a Job
  class Port
    attr_reader :job, :type, :name

    # @param job [SideJob::Job, SideJob::Worker]
    # @param type [:in, :out] Specifies whether it is input or output port
    # @param name [Symbol,String] Port names should match [a-zA-Z0-9_]+
    def initialize(job, type, name)
      @job = job
      @type = type.to_sym
      @name = name.to_sym
      raise "Invalid port name: #{@name}" if @name !~ /^[a-zA-Z0-9_]+$/
    end

    # @return [Boolean] True if two ports are equal
    def ==(other)
      other.is_a?(Port) && @job == other.job && @type == other.type && @name == other.name
    end

    # @see #==
    def eql?(other)
      return self == other
    end

    # @return [Boolean] Returns true if the port exists.
    def exists?
      ! mode.nil?
    end

    # @return [Symbol, nil] The port mode or nil if the port is invalid
    def mode
      mode = SideJob.redis.hget("#{@job.redis_key}:#{type}ports:mode", @name)
      mode = mode.to_sym if mode
      mode
    end

    # Returns the number of items waiting on this port.
    # @return [Fixnum]
    def size
      SideJob.redis.llen(redis_key)
    end

    # Returns whether {#read} will return data.
    # @return [Boolean] True if there is data to read.
    def data?
      size > 0 || default?
    end

    # Returns the port default value. Use {#default?} to distinguish between a null
    # default value and no default.
    # @return [Object, nil] The default value on the port or nil if none
    def default
      parse_json SideJob.redis.hget("#{@job.redis_key}:#{type}ports:default", @name)
    end

    # Returns if the port has a default value.
    # @return [Boolean] True if the port has a default value
    def default?
      SideJob.redis.hexists("#{@job.redis_key}:#{type}ports:default", @name)
    end

    # Write data to the port. If port in an input port, runs the job.
    # The default operating mode for a port is :queue which means packets are read/written as a FIFO queue.
    # In :memory mode, writes do not enter the queue and instead overwrite the default port value.
    # @param data [Object] JSON encodable data to write to the port
    def write(data)
      case mode
        when :queue
          SideJob.redis.rpush redis_key, data.to_json
        when :memory
          SideJob.redis.hset "#{@job.redis_key}:#{type}ports:default", @name, data.to_json
        else
          raise "Missing port #{@name} or invalid mode #{mode}"
      end

      @job.run if type == :in

      log('write', data)
      self
    end

    # Reads the oldest data from the port. Returns the default value if no data and there is a default.
    # @return [Object] First data from port
    # @raise [EOFError] Error raised if no data to be read
    def read
      data = SideJob.redis.lpop(redis_key)
      if data
        data = parse_json(data)
      else
        if default?
          data = default
        else
          raise EOFError
        end
      end

      log('read', data)
      data
    end

    include Enumerable
    # Iterate over port data. Default values are not returned.
    # @yield [Object] Each data from port
    def each(&block)
      while size > 0 do
        yield read
      end
    rescue EOFError
    end

    # Returns the redis key used for storing inputs or outputs from a port name
    # @return [String] Redis key
    def redis_key
      "#{@job.redis_key}:#{@type}:#{@name}"
    end
    alias :to_s :redis_key

    # @return [Fixnum] Hash value for port
    def hash
      redis_key.hash
    end

    private

    # Wrapper around JSON.parse to also handle primitive types.
    # @param data [String, nil] Data to parse
    # @return [Object, nil]
    def parse_json(data)
      data = JSON.parse("[#{data}]")[0] if data
      data
    end

    # Log a read or write on the port.
    def log(type, data)
      log = {data: data}
      log[:by] = @job.by if @job.by
      log["#{@type}port"] = @name
      @job.log type, log
    end
  end
end
