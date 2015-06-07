require 'delegate'

module SideJob
  # Represents an input or output port from a Job
  class Port
    # Returned by {#read} and {#default} to indicate no data
    class None; end

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

    # Returns the port default value. See {.decode_data} for details about the return value.
    # @return [Delegator, None] The default value on the port or {SideJob::Port::None} if none
    def default
      self.class.decode_data SideJob.redis.hget("#{@job.redis_key}:#{type}ports:default", @name)
    end

    # Returns if the port has a default value.
    # @return [Boolean] True if the port has a default value
    def default?
      SideJob.redis.hexists("#{@job.redis_key}:#{type}ports:default", @name)
    end

    # Sets the port default value.
    # @param val [Object, None] New JSON encodable default value or None to clear the default
    def default=(val)
      if val == None
        SideJob.redis.hdel "#{@job.redis_key}:#{type}ports:default", @name
      else
        SideJob.redis.hset "#{@job.redis_key}:#{type}ports:default", @name, self.class.encode_data(val)
      end
    end

    # Returns the connected port channels.
    # @return [Array<String>] List of port channels
    def channels
      JSON.parse(SideJob.redis.hget("#{@job.redis_key}:#{type}ports:channels", @name)) rescue []
    end

    # Set the channels connected to the port.
    # @param channels [Array<String>] Port channels
    def channels=(channels)
      SideJob.redis.multi do |multi|
        if channels && channels.length > 0
          multi.hset "#{@job.redis_key}:#{type}ports:channels", @name, channels.to_json
        else
          multi.hdel "#{@job.redis_key}:#{type}ports:channels", @name
        end

        if type == :in
          channels.each do |chan|
            multi.sadd "channel:#{chan}", @job.id
          end
        end
      end
    end

    # Write data to the port. If port in an input port, runs the job.
    # @param data [Object] JSON encodable data to write to the port
    def write(data)
      # For {SideJob::Worker#for_inputs}, if this is set, we instead set the port default on writes
      if Thread.current[:sidejob_port_write_default]
        self.default = data
      else
        SideJob.redis.rpush redis_key, self.class.encode_data(data)
      end
      @job.run(parent: type != :in) # run job if inport otherwise run parent
      log(write: [ { port: self, data: [data] } ])

      if type == :out
        channels.each do |chan|
          SideJob.publish chan, data
        end
      end
    end

    # Reads the oldest data from the port. See {.decode_data} for details about the wrapped return value.
    # Returns the {#default} if there is no port data and there is a default.
    # Returns {SideJob::Port::None} if there is no port data and no default.
    # @return [Delegator, None] First data from port or {SideJob::Port::None} if there is no data and no default
    def read
      data = SideJob.redis.lpop(redis_key)
      if data
        data = self.class.decode_data(data)
      elsif default?
        data = default
      else
        return None
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

      # Get source port data and default
      (default, data, trash) = result = SideJob.redis.multi do |multi|
        multi.hget("#{@job.redis_key}:#{@type}ports:default", @name)
        # get all and empty the port of all data
        multi.lrange redis_key, 0, -1
        multi.del redis_key
      end

      default = result[0]
      data = result[1]

      return data unless data.length > 0 || default

      # Get destination port defaults
      port_defaults = SideJob.redis.multi do |multi|
        # port defaults
        ports.each { |port| multi.hget("#{port.job.redis_key}:#{port.type}ports:default", port.name) }
      end

      SideJob.redis.multi do |multi|
        if data.length > 0
          ports.each_with_index do |port, i|
            multi.rpush port.redis_key, data
          end
        end

        if default
          ports.each_with_index do |port, i|
            if default != port_defaults[i]
              multi.hset "#{port.job.redis_key}:#{port.type}ports:default", port.name, default
            end
          end
        end
      end

      data.map! {|x| self.class.decode_data(x)}
      if data.length > 0
        log(read: [{ port: self, data: data }], write: ports.map { |port| {port: port, data: data} })

        # Publish to destination channels
        ports.each do |port|
          if port.type == :out
            port.channels.each do |chan|
              data.each { |x| SideJob.publish chan, x }
            end
          end
        end
      end

      # Run the port job or parent only if something was changed
      ports.each_with_index do |port, i|
        if data.length > 0 || default != port_defaults[i]
          port.job.run(parent: port.type != :in)
        end
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

    # Encodes data as JSON with the current SideJob context.
    # @param data [Object] JSON encodable data
    # @return [String] The encoded JSON value
    def self.encode_data(data)
      if Thread.current[:sidejob_context]
        { context: Thread.current[:sidejob_context], data: data }.to_json
      else
        { data: data }.to_json
      end
    end

    # Decodes data encoded with {.encode_data}.
    # The value is returned as a Delegator object that behaves mostly like the underlying value.
    # Use {Delegator#__getobj__} to get directly at the underlying value.
    # The returned delegator object has a sidejob_context method that returns the SideJob context.
    # @param data [String, nil] Data to decode
    # @return [Delegator, None] The decoded value or {SideJob::Port::None} if data is nil
    def self.decode_data(data)
      if data
        data = JSON.parse(data)
        klass = Class.new(SimpleDelegator) do
          # Allow comparing two SimpleDelegator objects
          def ==(obj)
            return self.__getobj__ == obj.__getobj__ if obj.is_a?(SimpleDelegator)
            super
          end
        end
        klass.send(:define_method, :sidejob_context) do
          data['context'] || {}
        end
        klass.new(data['data'])
      else
        None
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

    # Check if the port exists, dynamically creating it if it does not exist and a * port exists for the job
    # @raise [RuntimeError] Error raised if port does not exist
    def check_exists
      return if SideJob.redis.sismember "#{@job.redis_key}:#{type}ports", @name
      dynamic = SideJob.redis.sismember("#{@job.redis_key}:#{type}ports", '*')
      raise "Job #{@job.id} does not have #{@type}port #{@name}!" unless dynamic
      dynamic_default = SideJob.redis.hget("#{@job.redis_key}:#{type}ports:default", '*')
      SideJob.redis.multi do |multi|
        multi.sadd "#{@job.redis_key}:#{type}ports", @name
        if dynamic_default
          multi.hset "#{@job.redis_key}:#{type}ports:default", @name, dynamic_default
        end
      end
    end
  end
end
