module SideJob
  # All workers should include SideJob::Worker and implement the perform method.
  # @see SideJob::JobMethods
  module Worker
    module ClassMethods
    end

    def self.included(base)
      base.class_eval do
        include Sidekiq::Worker
        include SideJob::JobMethods
      end
      base.extend(ClassMethods)
    end

    # Queues a child job
    # @see SideJob.queue
    def queue(queue, klass, options={})
      SideJob.queue(queue, klass, options.merge({parent: self}))
    end

    # Suspend the current worker
    # Will restart upon call to SideJob::Job#restart
    def suspend
      self.status = :suspended
    end

    # Sets multiple values
    # Merges data into a job's metadata
    # @param data [Hash{String => String}] Data to update
    def mset(data)
      SideJob.redis do |redis|
        redis.hmset "#{redis_key}:data", *(data.to_a.flatten(1))
      end
    end

    # Sets a single data in the job's metadata
    # @param field [String,Symbol] Field to set
    # @param value [String]
    def set(field, value)
      mset({field => value})
    end

    # Sets a single JSON encoded data in the job's metadata
    # @param field [String,Symbol] Field to get
    # @param value [Object] JSON-serializable object
    def set_json(field, value)
      return unless value
      set(field, JSON.generate(value))
    end

    # Loads data from the job's metadata
    # @param fields [Array<String,Symbol>] Fields to load or all fields if none specified
    # @return [Hash{String,Symbol => String}] Job's metadata with the fields specified
    def mget(*fields)
      SideJob.redis do |redis|
        if fields.length > 0
          values = redis.hmget("#{redis_key}:data", *fields)
          Hash[fields.zip(values)]
        else
          redis.hgetall "#{redis_key}:data"
        end
      end
    end

    # Gets a single data from the job's metadata
    # @param field [String,Symbol] Field to get
    # @return [String, nil] Value of the given data field or nil
    def get(field)
      mget(field)[field]
    end

    # Gets a single JSON encoded data from the job's metadata
    # @param field [String,Symbol] Field to get
    # @return [Object, nil] JSON parsed value of the given data field
    def get_json(field)
      data = get(field)
      if data
        JSON.parse(data)
      else
        nil
      end
    end

    # Helps with getting and storing configuration-like data from a port
    # The assumption is that a configuration port only cares about the last data received on it
    # The last data is also saved in to the state
    # If no data in on the input port, load from saved state
    # @param field [String,Symbol] Name of configuration field/port
    # @return [String, nil] Configuration value or nil
    def get_config(field)
      data = input(field).pop_all.first
      if data
        set(field, data)
      else
        data = get(field)
      end
      data
    end

    # @see #get_config
    # @param field [String,Symbol] Field to get
    # @return [Object, nil] JSON parsed value of the given configuration value
    def get_config_json(field)
      data = get_config(field)
      if data
        JSON.parse(data)
      else
        nil
      end
    end
  end
end
