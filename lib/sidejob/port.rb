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
      SideJob.redis do |redis|
        redis.llen redis_key
      end
    end

    # Push some data on to the port
    # If the port is an input port, wakes up the job so it has chance to process the data
    # If the port is an output port, wake up the parent job so it has a chance to process it
    # Records the port so that it can be retrieved by Port.all
    # @param data [Array<String>] List of data to push on to port
    def push(*data)
      data.each do |x|
        log = {data: x}
        log["#{@type}port"] = @name
        @job.log_push 'write', log
      end

      if @type == :in
        @job.restart
      else
        @job.parent.restart if @job.parent
      end

      SideJob.redis do |redis|
        redis.multi do |multi|
          multi.lpush redis_key, data
          multi.sadd "#{@job.redis_key}:#{@type}ports", @name # set to store all port names
        end
      end
      self
    end

    # JSON encodes all data before pushing
    # @see #push
    def push_json(*data)
      push *(data.map {|x| JSON.generate(x)})
    end

    # Pops oldest data from the port
    # @return [String, nil] First data from port or nil if no data exists
    def pop
      data = SideJob.redis do |redis|
        redis.rpop redis_key
      end

      if data
        log = {data: data}
        log["#{@type}port"] = @name
        @job.log_push 'read', log
      end

      data
    end

    # Pops all data from the port
    # @return [Array<String>] All data from the port. Oldest data is last, most recent is first.
    def pop_all
      SideJob.redis do |redis|
        redis.watch(redis_key) do
          redis.multi do |multi|
            multi.lrange redis_key, 0, -1
            multi.del redis_key
          end
        end
      end[0]
    end

    # JSON decodes popped data
    # @see #pop
    def pop_json
      data = pop
      if data
        JSON.parse(data)
      else
        nil
      end
    end

    # Pops and JSON decodes all data from the port
    # @see #pop_all
    def pop_all_json
      pop_all.map { |data| JSON.parse(data) }
    end

    # Returns the redis key used for storing inputs or outputs from a port name
    # @return [String] Redis key
    def redis_key
      "#{@job.redis_key}:#{@type}:#{@name}"
    end

    def hash
      redis_key.hash
    end

    def to_s
      redis_key
    end

    # Returns all ports that have had been pushed to for the given job and type
    # @param job [SideJob::Job, SideJob::Worker]
    # @param type [:in, :out] Specifies whether it is input or output port
    # @return [Array<SideJob::Port>] All pushed to ports for the given job and type
    def self.all(job, type)
      SideJob.redis do |redis|
        redis.smembers("#{job.redis_key}:#{type}ports").map {|name| SideJob::Port.new(job, type, name)}
      end
    end

    # Deletes all Redis keys for all ports of the given job/type
    # @param job [SideJob::Job, SideJob::Worker]
    # @param type [:in, :out] Specifies whether it is input or output port
    def self.delete_all(job, type)
      SideJob.redis do |redis|
        redis.del ["#{job.redis_key}:#{type}ports"] +
                     redis.smembers("#{job.redis_key}:#{type}ports").map {|name| "#{job.redis_key}:#{type}:#{name}"}
      end
    end
  end
end
