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
      self == other
    end

    # @return [Boolean] Returns true if the port exists.
    def exists?
      ! mode.nil?
    end

    # Reset the port options. Currently supported options are mode and default.
    # @param options [Hash] New port options
    def options=(options)
      options = options.stringify_keys
      SideJob.redis.multi do |multi|
        multi.hset "#{@job.redis_key}:#{type}ports:mode", @name, options['mode'] || 'queue'
        if options.has_key?('default')
          multi.hset "#{@job.redis_key}:#{type}ports:default", @name, options['default'].to_json
        else
          multi.hdel "#{@job.redis_key}:#{type}ports:default", @name
        end
      end
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

    # Returns the port default value. To distinguish a null default value vs no default, use json: true or {#default?}.
    # @param json [Boolean] If true, returns the default value as a JSON encoded string (default false)
    # @return [String, Object, nil] The default value on the port or nil if none
    def default(json: false)
      default = SideJob.redis.hget("#{@job.redis_key}:#{type}ports:default", @name)
      if json
        default
      else
        parse_json default
      end
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
          @job.run if type == :in
        when :memory
          SideJob.redis.hset "#{@job.redis_key}:#{type}ports:default", @name, data.to_json
        else
          raise "Missing port #{@name} or invalid mode #{mode}"
      end

      log('write', data)
      self
    end

    # Reads the oldest data from the port. Returns the default value if no data and there is a default.
    # @return [Object] First data from port
    # @raise [EOFError] Error raised if no data to be read
    def read
      data = SideJob.redis.lpop(redis_key) || default(json: true)
      raise EOFError unless data
      data = parse_json(data)
      log('read', data)
      data
    end

    # Connects this port to a number of other ports.
    # All data is read from the current port and written to the destination ports.
    # If the current port has a default value, the default is copied to all destination ports.
    # @param ports [Array<SideJob::Port>, SideJob::Port] Destination port(s)
    def connect_to(ports)
      ports = [ports] unless ports.is_a?(Array)
      ports_by_mode = ports.group_by {|port| port.mode}

      default = default(json: true)

      # empty the port of all data
      data = SideJob.redis.multi do |multi|
        multi.lrange redis_key, 0, -1
        multi.del redis_key
      end[0]

      to_run = Set.new

      SideJob.redis.multi do |multi|
        if data.length > 0
          (ports_by_mode[:queue] || []).each do |port|
            multi.rpush port.redis_key, data
            to_run.add port.job if port.type == :in
          end
          if ! default
            (ports_by_mode[:memory] || []).each do |port|
              multi.hset "#{port.job.redis_key}:#{port.type}ports:default", port.name, data.last
            end
          end
        end

        if default
          ports.each do |port|
            multi.hset "#{port.job.redis_key}:#{port.type}ports:default", port.name, default
          end
        end
      end

      to_run.each { |job| job.run }
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
      raise "Invalid json #{data}" if data && ! data.is_a?(String)
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
