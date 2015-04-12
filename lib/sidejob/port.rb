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
      check_exists
    end

    # @return [Boolean] True if two ports are equal
    def ==(other)
      other.is_a?(Port) && @job == other.job && @type == other.type && @name == other.name
    end

    # @see #==
    def eql?(other)
      self == other
    end

    # Returns the port options. Currently supported options are mode and default.
    # @return [Hash] Port options
    def options
      opts = {mode: mode}
      opts[:default] = default if default?
      opts
    end

    # Reset the port options. Currently supported options are mode and default.
    # @param options [Hash] New port options
    def options=(options)
      options = options.symbolize_keys
      SideJob.redis.multi do |multi|
        multi.hset "#{@job.redis_key}:#{type}ports:mode", @name, options[:mode] || :queue
        if options.has_key?(:default)
          multi.hset "#{@job.redis_key}:#{type}ports:default", @name, options[:default].to_json
        else
          multi.hdel "#{@job.redis_key}:#{type}ports:default", @name
        end
      end
    end

    # @return [Symbol, nil] The port mode or nil if the port is invalid
    def mode
      mode = SideJob.redis.hget("#{@job.redis_key}:#{type}ports:mode", @name)
      mode ? mode.to_sym : nil
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

    # Returns the port default value. To distinguish a null default value vs no default, use {#default?}.
    # @return [Object, nil] The default value on the port or nil if none
    def default
      val = SideJob.redis.hget("#{@job.redis_key}:#{type}ports:default", @name)
      val ? parse_json(val) : nil
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
      log(write: [ { port: self, data: [data] } ])
    end

    # Reads the oldest data from the port. Returns the default value if no data and there is a default.
    # @return [Object] First data from port
    # @raise [EOFError] Error raised if no data to be read
    def read
      data = SideJob.redis.lpop(redis_key)
      if data
        data = parse_json(data)
      elsif default?
        data = default
      else
        raise EOFError unless data
      end

      log(read: [ { port: self, data: [data] } ])

      data
    end

    # Connects this port to a number of other ports.
    # All data is read from the current port and written to the destination ports.
    # If the current port has a default value, the default is copied to all destination ports.
    # @param ports [Array<SideJob::Port>, SideJob::Port] Destination port(s)
    # @return [Array<Object>] Returns all data on current port
    def connect_to(ports)
      ports = [ports] unless ports.is_a?(Array)
      ports_by_mode = ports.group_by {|port| port.mode}

      default = SideJob.redis.hget("#{@job.redis_key}:#{type}ports:default", @name)

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

      data.map! {|x| parse_json x}
      if data.length > 0
        log(read: [{ port: self, data: data }], write: ports.map { |port| {port: port, data: data} })
      end

      if data.length > 0 || default
        ports.each { |port| port.job.run if port.type == :in }
      end

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

    # Groups all port reads and writes within the block into a single logged event.
    def self.log_group(&block)
      outermost = ! Thread.current[:sidejob_port_group]
      Thread.current[:sidejob_port_group] ||= {read: {}, write: {}} # port -> [data]
      yield
    ensure
      if outermost
        self._really_log Thread.current[:sidejob_port_group]
        Thread.current[:sidejob_port_group] = nil
      end
    end

    private

    def self._really_log(entry)
      return unless entry && (entry[:read].length > 0 || entry[:write].length > 0)

      log_entry = {}
      %i{read write}.each do |type|
        log_entry[type] = entry[type].map do |port, data|
          x = {job: port.job.id, data: data}
          x[:"#{port.type}port"] = port.name
          x
        end
      end

      SideJob.log log_entry
    end

    def log(data)
      entry = Thread.current[:sidejob_port_group] ? Thread.current[:sidejob_port_group] : {read: {}, write: {}}
      %i{read write}.each do |type|
        (data[type] || []).each do |x|
          entry[type][x[:port]] ||= []
          entry[type][x[:port]].concat JSON.parse(x[:data].to_json) # serialize/deserialize to do a deep copy
        end
      end
      if ! Thread.current[:sidejob_port_group]
        self.class._really_log(entry)
      end
    end

    # Wrapper around JSON.parse to also handle primitive types.
    # @param data [String] Data to parse
    # @return [Object, nil]
    def parse_json(data)
      JSON.parse("[#{data}]")[0]
    end

    # Check if the port exists, dynamically creating it if it does not exist and a * port exists for the job
    # @raise [RuntimeError] Error raised if port does not exist
    def check_exists
      return if SideJob.redis.hexists "#{@job.redis_key}:#{type}ports:mode", @name
      dynamic_mode = SideJob.redis.hget("#{@job.redis_key}:#{type}ports:mode", '*')
      raise "Job #{@job.id} does not have #{@type}port #{@name}!" unless dynamic_mode
      dynamic_default = SideJob.redis.hget("#{@job.redis_key}:#{type}ports:default", '*')
      SideJob.redis.multi do |multi|
        multi.hset "#{@job.redis_key}:#{type}ports:mode", @name, dynamic_mode
        if dynamic_default
          multi.hset "#{@job.redis_key}:#{type}ports:default", @name, dynamic_default
        end
      end
    end
  end
end
