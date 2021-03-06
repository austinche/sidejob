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
      check_exists
      SideJob.redis.get "#{redis_key}:status"
    end

    # Set the job status.
    # @param status [String] The new job status
    def status=(status)
      check_exists
      oldstatus = SideJob.redis.getset("#{redis_key}:status", status)
      if oldstatus != status && worker_config['status_publish'] != false
        SideJob::Port.group(log: false) do
          publish({status: status})
        end
      end
    end

    # Returns all aliases for the job.
    # @return [Array<String>] Job aliases
    def aliases
      SideJob.redis.smembers "#{redis_key}:aliases"
    end

    # Add an alias for the job.
    # @param name [String] Alias for the job. Must begin with an alphabetic character.
    # @raise [RuntimeError] Error if name is invalid or the name already refers to another job
    def add_alias(name)
      check_exists
      raise "#{name} is not a valid alias" unless name =~ /^[[:alpha:]]/
      current = SideJob.redis.hget('jobs:aliases', name)
      if current
        raise "#{name} is already used by job #{current}"  if current.to_i != id
      else
        SideJob.redis.multi do |multi|
          multi.hset 'jobs:aliases', name, id
          multi.sadd "#{redis_key}:aliases", name
        end
      end
    end

    # Remove an alias for the job.
    # @param name [String] Alias to remove for the job
    # @raise [RuntimeError] Error if name is not an alias for this job
    def remove_alias(name)
      check_exists
      raise "#{name} is not an alias for job #{id}" unless SideJob.redis.sismember("#{redis_key}:aliases", name)
      SideJob.redis.multi do |multi|
        multi.hdel 'jobs:aliases', name
        multi.srem "#{redis_key}:aliases", name
      end
    end

    # Run the job.
    # This method ensures that the job runs at least once from the beginning.
    # If the job is currently running, it will run again.
    # Just like sidekiq, we make no guarantees that the job will not be run more than once.
    # Unless force is set, the job will only be run if the status is running, queued, suspended, or completed.
    # @param parent [Boolean] Whether to run parent job instead of this one
    # @param force [Boolean] Whether to run if job is terminated (default false)
    # @param at [Time, Float] Time to schedule the job, otherwise queue immediately
    # @param wait [Float] Run in the specified number of seconds
    # @return [SideJob::Job, nil] The job that was run or nil if no job was run
    def run(parent: false, force: false, at: nil, wait: nil)
      if parent
        pj = self.parent
        return pj ? pj.run(force: force, at: at, wait: wait) : nil
      end

      return nil unless force || %w{running queued suspended completed}.include?(status)

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

    # Returns if job and all children are terminated.
    # @return [Boolean] True if this job and all children recursively are terminated
    def terminated?
      return false if status != 'terminated'
      children.each_value do |child|
        return false unless child.terminated?
      end
      return true
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

    # Queues a child job, setting parent and by to self.
    # @see SideJob.queue
    def queue(queue, klass, **options)
      check_exists
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
      SideJob.find(SideJob.redis.get("#{redis_key}:parent"))
    end

    # Disown a child job so that it no longer has a parent.
    # @param name_or_job [String, SideJob::Job] Name or child job to disown
    def disown(name_or_job)
      if name_or_job.is_a?(SideJob::Job)
        job = name_or_job
        name = children.rassoc(job)
        raise "Job #{id} cannot disown job #{job.id} as it is not a child" unless name
      else
        name = name_or_job
        job = child(name)
        raise "Job #{id} cannot disown non-existent child #{name}" unless job
      end

      SideJob.redis.multi do |multi|
        multi.del "#{job.redis_key}:parent"
        multi.hdel "#{redis_key}:children", name
      end
    end

    # Adopt a parent-less job as a child of this job.
    # @param orphan [SideJob::Job] Job that has no parent
    # @param name [String] Name of child job (must be unique among children)
    def adopt(orphan, name)
      check_exists
      raise "Job #{id} cannot adopt itself as a child" if orphan == self
      raise "Job #{id} cannot adopt job #{orphan.id} as it already has a parent" unless orphan.parent.nil?
      raise "Job #{id} cannot adopt job #{orphan.id} as child name #{name} is not unique" if name.nil? || ! child(name).nil?

      SideJob.redis.multi do |multi|
        multi.set "#{orphan.redis_key}:parent", id.to_json
        multi.hset "#{redis_key}:children", name, orphan.id
      end
    end

    # Deletes the job and all children jobs (recursively) if all are terminated.
    # @return [Boolean] Whether the job was deleted
    def delete
      return false unless terminated?

      parent = self.parent
      parent.disown(self) if parent

      children = self.children
      aliases = self.aliases

      # delete all SideJob keys and disown all children
      ports = inports.map(&:redis_key) + outports.map(&:redis_key)
      SideJob.redis.multi do |multi|
        multi.srem 'jobs', id
        multi.del redis_key
        multi.del ports + %w{worker status state aliases parent children inports outports inports:default outports:default inports:channels outports:channels created_at created_by ran_at}.map {|x| "#{redis_key}:#{x}" }
        children.each_value { |child| multi.hdel child.redis_key, 'parent' }
        aliases.each { |name| multi.hdel('jobs:aliases', name) }
      end

      # recursively delete all children
      children.each_value do |child|
        child.delete
      end

      publish({deleted: true})
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

    # Returns
    # @return [Hash]
    def info
      check_exists
      data = SideJob.redis.multi do |multi|
        multi.get "#{redis_key}:worker"
        multi.get "#{redis_key}:created_by"
        multi.get "#{redis_key}:created_at"
        multi.get "#{redis_key}:ran_at"
      end

      worker = JSON.parse(data[0])
      {
          queue: worker['queue'], class: worker['class'], args: worker['args'],
          created_by: data[1], created_at: data[2], ran_at: data[3],
      }
    end

    # Returns the entirety of the job's internal state.
    # @return [Hash{String => Object}] Job internal state
    def state
      check_exists
      state = SideJob.redis.hgetall("#{redis_key}:state")
      state.update(state) {|k,v| JSON.parse("[#{v}]")[0]}
      state
    end

    # Returns some data from the job's internal state.
    # @param key [Symbol,String] Retrieve value for the given key
    # @return [Object,nil] Value from the job state or nil if key does not exist
    def get(key)
      check_exists
      val = SideJob.redis.hget("#{redis_key}:state", key)
      val ? JSON.parse("[#{val}]")[0] : nil
    end

    # Sets values in the job's internal state.
    # @param data [Hash{String,Symbol => Object}] Data to update: objects should be JSON encodable
    # @raise [RuntimeError] Error raised if job no longer exists
    def set(data)
      check_exists
      return unless data.size > 0
      SideJob.redis.hmset "#{redis_key}:state", *(data.map {|k,v| [k, v.to_json]}.flatten)
    end

    # Unsets some fields in the job's internal state.
    # @param fields [Array<String,Symbol>] Fields to unset
    # @raise [RuntimeError] Error raised if job no longer exists
    def unset(*fields)
      return unless fields.length > 0
      SideJob.redis.hdel "#{redis_key}:state", fields
    end

    # Acquire a lock on the job with a given expiration time.
    # @param ttl [Fixnum] Lock expiration in seconds
    # @param retries [Fixnum] Number of attempts to retry getting lock
    # @param retry_delay [Float] Maximum seconds to wait (actual will be randomized) before retry getting lock
    # @return [String, nil] Lock token that should be passed to {#unlock} or nil if lock was not acquired
    def lock(ttl, retries: 3, retry_delay: 0.2)
      check_exists
      retries.times do
        token = SecureRandom.uuid
        if SideJob.redis.set("#{redis_key}:lock", token, {nx: true, ex: ttl})
          return token # lock acquired
        else
          sleep Random.rand(retry_delay)
        end
      end
      return nil # lock not acquired
    end

    # Refresh the lock expiration.
    # @param ttl [Fixnum] Refresh lock expiration for the given time in seconds
    # @return [Boolean] Whether the timeout was set
    def refresh_lock(ttl)
      check_exists
      SideJob.redis.expire "#{redis_key}:lock", ttl
    end

    # Unlock job by deleting the lock only if it equals the lock token.
    # @param token [String] Token returned by {#lock}
    # @return [Boolean] Whether the job was unlocked
    def unlock(token)
      check_exists
      return SideJob.redis.eval('
        if redis.call("get",KEYS[1]) == ARGV[1] then
          return redis.call("del",KEYS[1])
        else
          return 0
        end', { keys: ["#{redis_key}:lock"], argv: [token] }) == 1
    end

    # Publishes a message to the job's channel.
    # @param message [Object] JSON encodable message
    def publish(message)
      SideJob.publish "/sidejob/job/#{id}", message
    end

    private

    # Queue or schedule this job using sidekiq.
    # @param time [Time, Float, nil] Time to schedule the job if specified
    def sidekiq_queue(time=nil)
      # Don't need to queue if a worker is already in process of running
      return if SideJob.redis.exists "#{redis_key}:lock:worker"

      worker = JSON.parse(SideJob.redis.get("#{redis_key}:worker"))
      # Don't need to queue if the job is already in the queue (this does not include scheduled jobs)
      # When Sidekiq pulls job out from scheduled set, we can still get the same job queued multiple times
      # but the server middleware handles it
      return if Sidekiq::Queue.new(worker['queue']).find_job(@id)

      if ! SideJob::Worker.config(worker['queue'], worker['class'])
        self.status = 'terminated'
        raise "Worker no longer registered for #{klass} in queue #{worker['queue']}"
      end
      item = {'jid' => id, 'queue' => worker['queue'], 'class' => worker['class'], 'args' => worker['args'] || [], 'retry' => false}
      item['at'] = time if time && time > Time.now.to_f
      Sidekiq::Client.push(item)
    end

    # Return all ports of the given type
    def all_ports(type)
      SideJob.redis.smembers("#{redis_key}:#{type}ports").reject {|name| name == '*'}.map {|name| SideJob::Port.new(self, type, name)}
    end

    # Return the worker configuration
    # @return [Hash] Worker config for the job
    def worker_config
      worker = JSON.parse(SideJob.redis.get("#{redis_key}:worker"))
      SideJob::Worker.config(worker['queue'], worker['class']) || {}
    end

    # Sets the input/outputs ports for the job and overwrites all current options.
    # The ports are merged with the worker configuration.
    # Any current ports that are not in the new port set are deleted (including any data on those ports).
    # @param type [:in, :out] Input or output ports
    # @param ports [Hash{Symbol,String => Hash}] Port configuration. Port name to options.
    def set_ports(type, ports)
      check_exists
      current = SideJob.redis.smembers("#{redis_key}:#{type}ports") || []

      ports ||= {}
      ports = (worker_config["#{type}ports"] || {}).merge(ports.dup.stringify_keys)
      ports.each_key do |port|
        ports[port] = ports[port].stringify_keys
      end

      SideJob.redis.multi do |multi|
        # remove data from old ports
        (current - ports.keys).each do |port|
          multi.del "#{redis_key}:#{type}:#{port}"
        end

        multi.del "#{redis_key}:#{type}ports"
        multi.sadd "#{redis_key}:#{type}ports", ports.keys if ports.length > 0

        # replace port defaults
        defaults = ports.map do |port, options|
          if options.has_key?('default')
            [port, SideJob::Port.encode_data(options['default'])]
          else
            nil
          end
        end.compact.flatten(1)
        multi.del "#{redis_key}:#{type}ports:default"
        multi.hmset "#{redis_key}:#{type}ports:default", *defaults if defaults.length > 0

        # replace port channels
        channels = ports.map do |port, options|
          if options.has_key?('channels')
            options['channels'].each do |channel|
              multi.sadd "channel:#{channel}", id
            end
            [port, options['channels'].to_json]
          else
            nil
          end
        end.compact.flatten(1)
        multi.del "#{redis_key}:#{type}ports:channels"
        multi.hmset "#{redis_key}:#{type}ports:channels", *channels if channels.length > 0
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

    # @param alias_or_id [String, Integer] Job alias or id
    def initialize(alias_or_id)
      @id = (SideJob.redis.hget('jobs:aliases', alias_or_id.to_s) || alias_or_id).to_i
      check_exists
    end
  end
end
