module SideJob
  # Represents an input or output port from a Job
  class Port
    attr_reader :job, :type, :name, :mode

    # @param job [SideJob::Job, SideJob::Worker]
    # @param type [:in, :out] Specifies whether it is input or output port
    # @param name [Symbol,String] Port names should match [a-zA-Z0-9_]+
    # @param options [Hash] Port options like changing mode to memory
    def initialize(job, type, name, options={})
      @job = job
      @type = type
      @name = name
      @options = options
      load_options
      raise "Invalid port name: #{@name}" if @name !~ /^[a-zA-Z0-9_]+$/
    end

    # @return [Boolean] True if two ports are equal
    def ==(other)
      other.is_a?(Port) && job == other.job && type == other.type && name.to_s == other.name.to_s
    end

    # @see #==
    def eql?(other)
      return self == other
    end

    # Returns the number of items waiting on this port
    # @return [Fixnum]
    def size
      SideJob.redis.llen redis_key
    end

    # @return [Boolean] True if there is data to read
    def data?
      SideJob.redis.exists redis_key
    end

    # Change the operating mode for the port.
    # The default operating mode for a port is :queue which means packets are read/written as a FIFO queue.
    # In :memory mode, only one value is stored on a port with more recent values overwriting older values.
    # Reads do not clear out the data in :memory mode.
    # @param mode [:queue, :memory] The new operating mode for the port
    def mode=(mode)
      update_options(:mode, mode)
    end

    # Write data to the port
    # If the port is an input port, wakes up the job so it has chance to process the data
    # If the port is an output port, wake up the parent job so it has a chance to process it
    # @param data [Array<Object>|Object] Data to write to the port: objects should be JSON encodable
    def write(*data)
      data = [data] unless data.is_a?(Array)
      return if data.length == 0

      if mode == :memory
        SideJob.redis.multi do |multi|
          multi.del redis_key
          multi.rpush redis_key, data.last.to_json
        end
      else
        SideJob.redis.rpush redis_key, data.map {|x| x.to_json}
      end

      data.each do |x|
        log('write', x)
      end

      if @type == :in
        @job.run
      else
        @job.parent.run if @job.parent
      end
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
        log('read', data)
      else
        raise EOFError
      end
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

    # load port options
    def load_options
      if @options['mode']
        @mode = @options['mode'].to_sym
      else
        @mode = :queue
      end
      raise "Invalid #{@mode} mode for output port #{@name}" if @mode == :memory && @type == :out
    end

    def update_options(key, value)
      @options[key.to_s] = value
      @job.set_port_options(@type, @name, @options)
      load_options
    end

    def log(type, data)
      log = {data: data}
      log[:by] = @job.by if @job.by
      log["#{@type}port"] = @name
      @job.log type, log
    end
  end
end
