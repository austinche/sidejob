module SideJob
  # Methods shared between {SideJob::Job} and {SideJob::Worker}.
  module JobMethods
    attr_reader :id

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
      SideJob.redis.sismember 'jobs', id
    end

    # Retrieve the job's status.
    # @return [String] Job status
    def status
      get(:status)
    end

    # Set the job status.
    # @param status [String] The new job status
    def status=(status)
      set({status: status})
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
    # @param parent [Boolean] Whether to run parent job instead of this one
    # @param force [Boolean] Whether to run if job is terminated (default false)
    # @param at [Time, Float] Time to schedule the job, otherwise queue immediately
    # @param wait [Float] Run in the specified number of seconds
    # @return [SideJob::Job, nil] The job that was run or nil if no job was run
    def run(parent: false, force: false, at: nil, wait: nil)
      check_exists

      if parent
        pj = self.parent
        return pj ? pj.run(force: force, at: at, wait: wait) : nil
      end

      case status
        when 'terminating', 'terminated'
          return nil unless force
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

    # Queues a child job, setting parent and by to self.
    # @see SideJob.queue
    def queue(queue, klass, **options)
      SideJob.queue(queue, klass, options.merge({parent: self, by: "job:#{id}"}))
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

    # Returns the parent job.
    # @return [SideJob::Job, nil] Parent job or nil if none
    def parent
      parent = get(:parent)
      parent = SideJob.find(parent) if parent
      parent
    end

    # Disown a child job so that it no longer has a parent.
    # @param name [String] Name of child job to disown
    def disown(name)
      job = child(name)
      raise "Job #{id} cannot disown non-existent child #{name}" unless job
      SideJob.redis.multi do |multi|
        multi.hdel job.redis_key, 'parent'
        multi.hdel "#{redis_key}:children", name
      end
    end

    # Adopt a parent-less job as a child of this job.
    # @param orphan [SideJob::Job] Job that has no parent
    # @param name [String] Name of child job (must be unique among children)
    def adopt(orphan, name)
      raise "Job #{id} cannot adopt itself as a child" if orphan == self
      raise "Job #{id} cannot adopt job #{orphan.id} as it already has a parent" unless orphan.parent.nil?
      raise "Job #{id} cannot adopt job #{orphan.id} as child name #{name} is not unique" if name.nil? || ! child(name).nil?

      SideJob.redis.multi do |multi|
        multi.hset orphan.redis_key, 'parent', id.to_json
        multi.hset "#{redis_key}:children", name, orphan.id
      end
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
        multi.srem 'jobs', id
        multi.del redis_key
        multi.del ports + %w{children inports:mode outports:mode inports:default outports:default}.map {|x| "#{redis_key}:#{x}" }
      end

      return true
    end

    # Returns an input port.
    # @param name [Symbol,String] Name of the port
    # @return [SideJob::Port]
    def input(name)
      SideJob::Port.new(self, :in, name)
    end

    # Returns an output port
    # @param name [Symbol,String] Name of the port
    # @return [SideJob::Port]
    def output(name)
      SideJob::Port.new(self, :out, name)
    end

    # Gets all input ports.
    # @return [Array<SideJob::Port>] Input ports
    def inports
      all_ports :in
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
      all_ports :out
    end

    # Sets the input ports for the job.
    # The ports are merged with the worker configuration.
    # Any current ports that are not in the new port set are deleted (including any data on those ports).
    # @param ports [Hash{Symbol,String => Hash}] Output port configuration. Port name to options.
    def outports=(ports)
      set_ports :out, ports
    end

    # Returns the entirety of the job's state with both standard and custom keys.
    # @return [Hash{String => Object}] Job state
    def state
      state = SideJob.redis.hgetall(redis_key)
      raise "Job #{id} does not exist!" if ! state
      state.update(state) {|k,v| JSON.parse("[#{v}]")[0]}
      state
    end

    # Returns some data from the job's state.
    # @param key [Symbol,String] Retrieve value for the given key
    # @return [Object,nil] Value from the job state or nil if key does not exist
    def get(key)
      val = SideJob.redis.hget(redis_key, key)
      val ? JSON.parse("[#{val}]")[0] : nil
    end

    # Sets values in the job's internal state.
    # @param data [Hash{String,Symbol => Object}] Data to update: objects should be JSON encodable
    # @raise [RuntimeError] Error raised if job no longer exists
    def set(data)
      check_exists
      return unless data.size > 0
      SideJob.redis.hmset redis_key, *(data.map {|k,v| [k, v.to_json]}.flatten)
    end

    # Unsets some fields in the job's internal state
    # @param fields [Array<String,Symbol>] Fields to unset
    # @raise [RuntimeError] Error raised if job no longer exists
    def unset(*fields)
      return unless fields.length > 0
      SideJob.redis.hdel redis_key, fields
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

    # Return all ports of the given type
    def all_ports(type)
      SideJob.redis.hkeys("#{redis_key}:#{type}ports:mode").reject {|name| name == '*'}.map {|name| SideJob::Port.new(self, type, name)}
    end

    # Sets the input/outputs ports for the job and overwrites all current options.
    # The ports are merged with the worker configuration.
    # Any current ports that are not in the new port set are deleted (including any data on those ports).
    # @param type [:in, :out] Input or output ports
    # @param ports [Hash{Symbol,String => Hash}] Port configuration. Port name to options.
    def set_ports(type, ports)
      current = SideJob.redis.hkeys("#{redis_key}:#{type}ports:mode") || []
      config = SideJob::Worker.config(get(:queue), get(:class))

      ports ||= {}
      ports = (config["#{type}ports"] || {}).merge(ports.dup.stringify_keys)
      ports.each_key do |port|
        ports[port] = ports[port].stringify_keys
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
      raise "Job #{id} does not exist!" unless exists?
    end
  end

  # Wrapper for a job which may not be in progress unlike SideJob::Worker.
  # @see SideJob::JobMethods
  class Job
    include JobMethods

    # @param id [Integer] Job id
    def initialize(id)
      @id = id.to_i
      check_exists
    end
  end
end
