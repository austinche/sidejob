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
      SideJob.redis.llen redis_key
    end

    # Write data to the port
    # If the port is an input port, wakes up the job so it has chance to process the data
    # If the port is an output port, wake up the parent job so it has a chance to process it
    # @param data [Array<Object>|Object] Data to write to the port: objects should be JSON encodable
    def write(*data)
      data = [data] unless data.is_a?(Array)
      return if data.length == 0

      SideJob.redis.multi do |multi|
        multi.rpush redis_key, data.map {|x| x.to_json}
        multi.sadd "#{@job.redis_key}:#{@type}ports", @name
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
    # @return [Object, nil] First data from port or nil if no data exists
    def read
      data = SideJob.redis.lpop redis_key
      if data
        data = JSON.parse("[#{data}]")[0] # enable parsing primitive types like strings, numbers
        log('read', data)
      end
      data
    end

    # Drains and returns all data from the port
    # @return [Array<Object>] All data from the port. Oldest data is first, most recent is last
    def drain
      data = SideJob.redis.multi do |multi|
        multi.lrange redis_key, 0, -1
        multi.del redis_key
      end[0]

      data.map! do |x|
        x = JSON.parse("[#{x}]")[0] # enable parsing primitive types like strings, numbers
        log('read', x)
        x
      end

      data
    end

    # Iterate over port data
    include Enumerable
    def each(&block)
      drain.each do |data|
        yield data
      end
    end

    # Returns the redis key used for storing inputs or outputs from a port name
    # @return [String] Redis key
    def redis_key
      "#{@job.redis_key}:#{@type}:#{@name}"
    end
    alias :to_s :redis_key

    def hash
      redis_key.hash
    end

    private

    def log(type, data)
      log = {data: data}
      log[:by] = @job.by if @job.by
      log["#{@type}port"] = @name
      @job.log type, log
    end
  end
end
