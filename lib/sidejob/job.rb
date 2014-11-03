module SideJob
  # Methods shared between {SideJob::Job} and {SideJob::Worker}.
  module JobMethods
    attr_reader :id
    attr_accessor :logger

    # @return [Boolean] True if two jobs or workers have the same id
    def ==(other)
      other.respond_to?(:id) && id == other.id
    end

    # @see #==
    def eql?(other)
      self == other
    end

    # @return [Fixnum] Hash value based on the id
    def hash
      id.hash
    end

    # @return [String] Prefix for all redis keys related to this job
    def redis_key
      "job:#{id}"
    end
    alias :to_s :redis_key

    # Returns if the job still exists.
    # @return [Boolean] Returns true if this job exists and has not been deleted
    def exists?
      SideJob.redis.hexists 'jobs', id
    end

    # If a job logger is defined, call the log method on it with the log entry. Otherwise, call {SideJob.log}.
    # @param entry [Hash] Log entry
    def log(entry)
      (@logger || SideJob).log(entry)
    end

    # Groups all port reads and writes within the block into a single logged event.
    # @param metadata [Hash] If provided, the metadata is merged into the final log entry
    def group_port_logs(metadata={}, &block)
      new_group = @logger.nil?
      @logger ||= GroupPortLogs.new
      @logger.add_metadata metadata
      yield
    ensure
      if new_group
        @logger.done
        @logger = nil
      end
    end

    # Retrieve the job's status.
    # @return [String] Job status
    def status
      SideJob.redis.get "#{redis_key}:status"
    end

    # Set the job status.
    # @param status [String] The new job status
    def status=(status)
      SideJob.redis.set "#{redis_key}:status", status
    end

    # Prepare to terminate the job. Sets status to 'terminating'.
    # Then queues the job so that its shutdown method if it exists can be run.
    # After shutdown, the status will be 'terminated'.
    # If the job is currently running, it will finish running first.
    # If the job is already terminated, it does nothing.
    # To start the job after termination, call {#run} with force: true.
    # @param recursive [Boolean] If true, recursively terminate all children (default false)
    # @return [SideJob::Job] self
    def terminate(recursive: false)
      if status != 'terminated'
        self.status = 'terminating'
        sidekiq_queue
      end
      if recursive
        children.each_value do |child|
          child.terminate(recursive: true)
        end
      end
      self
    end

    # Run the job.
    # This method ensures that the job runs at least once from the beginning.
    # If the job is currently running, it will run again.
    # Just like sidekiq, we make no guarantees that the job will not be run more than once.
    # Unless force is set, if the status is terminating or terminated, the job will not be run.
    # @param force [Boolean] Whether to run if job is terminated (default false)
    # @param at [Time, Float] Time to schedule the job, otherwise queue immediately
    # @param wait [Float] Run in the specified number of seconds
    # @return [SideJob::Job] self
    def run(force: false, at: nil, wait: nil)
      check_exists

      case status
        when 'terminating', 'terminated'
          return unless force
      end

      self.status = 'queued'

      time = nil
      if at
        time = at
        time = time.to_f if time.is_a?(Time)
      elsif wait
        time = Time.now.to_f + wait
      end
      sidekiq_queue(time)

      self
    end

    # Returns a child job by name.
    # @param name [Symbol, String] Child job name to look up
    # @return [SideJob::Job, nil] Child job or nil if not found
    def child(name)
      SideJob.find(SideJob.redis.hget("#{redis_key}:children", name))
    end

    # Returns all children jobs.
    # @return [Hash<String => SideJob::Job>] Children jobs by name
    def children
      SideJob.redis.hgetall("#{redis_key}:children").each_with_object({}) {|child, hash| hash[child[0]] = SideJob.find(child[1])}
    end

    # Returns all ancestor jobs.
    # @return [Array<SideJob::Job>] Ancestors (parent will be first and root job will be last)
    def ancestors
      SideJob.redis.lrange("#{redis_key}:ancestors", 0, -1).map { |id| SideJob.find(id) }
    end

    # Returns the parent job.
    # @return [SideJob::Job, nil] Parent job or nil if none
    def parent
      parent = SideJob.redis.lindex("#{redis_key}:ancestors", 0)
      parent = SideJob.find(parent) if parent
      parent
    end

    # Returns if job and all children are terminated.
    # @return [Boolean] True if this job and all children recursively are terminated
    def terminated?
      return false if status != 'terminated'
      children.each_value do |child|
        return false unless child.terminated?
      end
      return true
    end

    # Deletes the job and all children jobs (recursively) if all are terminated.
    # @return [Boolean] Whether the job was deleted
    def delete
      return false unless terminated?

      # recursively delete all children first
      children.each_value do |child|
        child.delete
      end

      # delete all SideJob keys
      ports = inports.map(&:redis_key) + outports.map(&:redis_key)
      SideJob.redis.multi do |multi|
        multi.hdel 'jobs', id
        multi.del ports + %w{status children ancestors inports:mode outports:mode inports:default outports:default}.map {|x| "#{redis_key}:#{x}" }
      end
      reload
      return true
    end

    # Returns an input port.
    # @param name [Symbol,String] Name of the port
    # @return [SideJob::Port]
    # @raise [RuntimeError] Error raised if port does not exist
    def input(name)
      get_port :in, name
    end

    # Returns an output port
    # @param name [Symbol,String] Name of the port
    # @return [SideJob::Port]
    # @raise [RuntimeError] Error raised if port does not exist
    def output(name)
      get_port :out, name
    end

    # Gets all input ports.
    # @return [Array<SideJob::Port>] Input ports
    def inports
      SideJob.redis.hkeys("#{redis_key}:inports:mode").map {|name| SideJob::Port.new(self, :in, name)}
    end

    # Sets the input ports for the job.
    # The ports are merged with the worker configuration.
    # Any current ports that are not in the new port set are deleted (including any data on those ports).
    # @param ports [Hash{Symbol,String => Hash}] Input port configuration. Port name to options.
    def inports=(ports)
      set_ports :in, ports
    end

    # Gets all output ports.
    # @return [Array<SideJob::Port>] Output ports
    def outports
      SideJob.redis.hkeys("#{redis_key}:outports:mode").map {|name| SideJob::Port.new(self, :out, name)}
    end

    # Sets the input ports for the job.
    # The ports are merged with the worker configuration.
    # Any current ports that are not in the new port set are deleted (including any data on those ports).
    # @param ports [Hash{Symbol,String => Hash}] Output port configuration. Port name to options.
    def outports=(ports)
      set_ports :out, ports
    end

    # Returns some data from the job's state.
    # The job state is cached for the lifetime of the job object. Call {#reload} if the state may have changed.
    # @param key [Symbol,String] Retrieve value for the given key
    # @return [Object,nil] Value from the job state or nil if key does not exist
    # @raise [RuntimeError] Error raised if job no longer exists
    def get(key)
      load_state
      @state[key.to_s]
    end

    # Clears the state and ports cache.
    def reload
      @state = nil
      @ports = nil
      @config = nil
    end

    # Returns the worker configuration for the job.
    # @see SideJob::Worker.config
    def config
      @config ||= SideJob::Worker.config(get(:queue), get(:class))
    end

    private

    # Queue or schedule this job using sidekiq.
    # @param time [Time, Float, nil] Time to schedule the job if specified
    def sidekiq_queue(time=nil)
      queue = get(:queue)
      klass = get(:class)
      args = get(:args)

      if ! SideJob.redis.hexists("workers:#{queue}", klass)
        self.status = 'terminated'
        raise "Worker no longer registered for #{klass} in queue #{queue}"
      end
      item = {'jid' => id, 'queue' => queue, 'class' => klass, 'args' => args || [], 'retry' => false}
      item['at'] = time if time && time > Time.now.to_f
      Sidekiq::Client.push(item)
    end

    # Returns an input or output port.
    # @param type [:in, :out] Input or output port
    # @param name [Symbol,String] Name of the port
    # @return [SideJob::Port]
    def get_port(type, name)
      port = SideJob::Port.new(self, type, name)
      raise "Unknown #{type}put port: #{name}" unless port.exists?
      port
    end

    # Sets the input/outputs ports for the job and overwrites all current options.
    # The ports are merged with the worker configuration.
    # Any current ports that are not in the new port set are deleted (including any data on those ports).
    # @param type [:in, :out] Input or output ports
    # @param ports [Hash{Symbol,String => Hash}] Port configuration. Port name to options.
    def set_ports(type, ports)
      current = SideJob.redis.hkeys("#{redis_key}:#{type}ports:mode") || []

      if ports
        ports = (ports || {}).stringify_keys
        ports.each_key {|port| ports[port] = ports[port].stringify_keys }
      else
        ports = config["#{type}ports"] || {}
      end

      SideJob.redis.multi do |multi|
        # remove data from old ports
        (current - ports.keys).each do |port|
          multi.del "#{redis_key}:#{type}:#{port}"
        end

        # completely replace the mode and default keys

        multi.del "#{redis_key}:#{type}ports:mode"
        modes = ports.map do |port, options|
          [port, options['mode'] || 'queue']
        end.flatten(1)
        multi.hmset "#{redis_key}:#{type}ports:mode", *modes if modes.length > 0

        defaults = ports.map do |port, options|
          if options.has_key?('default')
            [port, options['default'].to_json]
          else
            nil
          end
        end.compact.flatten(1)
        multi.del "#{redis_key}:#{type}ports:default"
        multi.hmset "#{redis_key}:#{type}ports:default", *defaults if defaults.length > 0
      end
    end

    # @raise [RuntimeError] Error raised if job no longer exists
    def check_exists
      raise "Job #{id} no longer exists!" unless exists?
    end

    def load_state
      if ! @state
        state = SideJob.redis.hget('jobs', id)
        raise "Job #{id} no longer exists!" if ! state
        @state = JSON.parse(state)
      end
      @state
    end
  end

  # Wrapper for a job which may not be in progress unlike SideJob::Worker.
  # @see SideJob::JobMethods
  class Job
    include JobMethods

    # @param id [String] Job id
    def initialize(id)
      @id = id
    end
  end

  # Logger that groups all port read/writes together.
  # @see {JobMethods#group_port_logs}
  class GroupPortLogs
    # If entry is not a port log, send it on to {SideJob.log}. Otherwise, collect the log until {#done} is called.
    # @param entry [Hash] Log entry
    def log(entry)
      if entry[:read] && entry[:write]
        # collect reads and writes by port and group data together
        @port_events ||= {read: {}, write: {}} # {job: id, <in|out>port: port} -> data array
        %i{read write}.each do |type|
          entry[type].each do |event|
            data = event.delete(:data)
            @port_events[type][event] ||= []
            @port_events[type][event].concat data
          end
        end
      else
        SideJob.log(entry)
      end
    end

    # Merges the collected port read and writes and send logs to {SideJob.log}.
    def done
      return unless @port_events && (@port_events[:read].length > 0 || @port_events[:write].length > 0)

      entry = {}
      %i{read write}.each do |type|
        entry[type] = @port_events[type].map do |port, data|
          port.merge({data: data})
        end
      end

      SideJob.log @metadata.merge(entry)
      @port_events = nil
    end

    # Add metadata fields to the final log entry.
    # @param metadata [Hash] Data to be merged with the existing metadata and final log entry
    def add_metadata(metadata)
      @metadata ||= {}
      @metadata.merge!(metadata)
    end
  end
end
