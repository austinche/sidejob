module SideJob
  # Represents an input or output port from a Job
  class Port
    attr_reader :job, :type, :name, :mode, :default

    # @param job [SideJob::Job, SideJob::Worker]
    # @param type [:in, :out] Specifies whether it is input or output port
    # @param name [Symbol,String] Port names should match [a-zA-Z0-9_]+
    # @param options [Hash] Port options like changing mode to memory
    def initialize(job, type, name, options={})
      @job = job
      @type = type.to_sym
      @name = name.to_sym
      raise "Invalid port name: #{@name}" if @name !~ /^[a-zA-Z0-9_]+$/

      # The default operating mode for a port is :queue which means packets are read/written as a FIFO queue.
      # In :memory mode, only one value is stored on a port with more recent values overwriting older values.
      # Reads do not clear out the data in :memory mode.
      if options['mode']
        @mode = options['mode'].to_sym
      else
        @mode = :queue
      end
      # Disallow setting output port to memory mode as it doesn't make sense
      raise "Invalid #{@mode} mode for output port #{@name}" if @mode == :memory && @type == :out

      # An input port can have a default value to return from {#read} when it's empty
      @default = options['default'] if options['default']
      raise "Cannot have a default value for output port #{@name}" if @default && @type == :out
    end

    # @return [Boolean] True if two ports are equal
    def ==(other)
      other.is_a?(Port) && job == other.job && type == other.type && name.to_s == other.name.to_s
    end

    # @see #==
    def eql?(other)
      return self == other
    end

    # Returns the number of items waiting on this port.
    # @return [Fixnum]
    def size
      length = SideJob.redis.llen(redis_key)
      return 1 if length == 0 && ! default.nil?
      length
    end

    # @return [Boolean] True if there is data to read.
    def data?
      size > 0
    end

    # Write data to the port.
    # @param data [Object] JSON encodable data to write to the port
    def write(data)
      SideJob.redis.multi do |multi|
        multi.del redis_key if mode == :memory
        multi.rpush redis_key, data.to_json
      end

      log('write', data)
      self
    end

    # Reads the oldest data from the port
    # @return [Object] First data from port
    # @raise [EOFError] Error raised if no data to be read
    def read
      if mode == :memory
        data = SideJob.redis.lrange redis_key, -1, -1
        data = data[0] if data
      else
        data = SideJob.redis.lpop redis_key
      end

      if data
        data = JSON.parse("[#{data}]")[0] # enable parsing primitive types like strings, numbers
      else
        data = default
        raise EOFError unless data
      end

      log('read', data)
      data
    end

    include Enumerable
    # Iterate over port data
    # For memory ports, at most one data is returned
    # @yield [Object] Each data from port
    def each(&block)
      if mode == :memory
        yield read
      else
        loop { yield read }
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

    # Log a read or write on the port.
    def log(type, data)
      log = {data: data}
      log[:by] = @job.by if @job.by
      log["#{@type}port"] = @name
      @job.log type, log
    end
  end
end
