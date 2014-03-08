module SideJob
  # Executes a flow graph written in the language parsed by SideJob::Parser
  # Options:
  #   graph: flow graph
  # Input ports:
  #   Ports specified in graph
  # Output ports
  #   Ports specified in graph
  class Graph
    include SideJob::Worker

    def perform(options)
      graph = get_json(:graph) || SideJob::Parser.parse(options['graph'])

      @jobs = {} # cache SideJob::Job objects by job name
      @to_restart = Set.new

      # make sure all jobs are started
      graph['jobs'].each_pair do |name, info|
        if info['jid']
          @jobs[name] = SideJob.find(info['jid'])
        else
          # start a new job
          job = queue(info['queue'], info['class'], info['args'] ? info['args'] : {})
          info['jid'] = job.jid
          set_json :graph, graph

          job.set(:name, name)
          @jobs[name] = job
        end
      end

      # now handle all connections
      graph['jobs'].each_pair do |name, info|
        next unless info['connections']

        job = @jobs[name]
        info['connections'].each_pair do |outport, targets|
          connect_ports(job.output(outport), targets)
        end
      end

      if graph['inports']
        graph['inports'].each_pair do |port, targets|
          connect_ports(input(port), targets)
        end
      end

      if graph['outports']
        graph['outports'].each_pair do |port, from|
          from.each do |source|
            get_port(:out, source).pop_all_to(output(port))
          end
        end
      end

      @to_restart.each do |job|
        job.restart
      end

      # we complete if all jobs are completed
      # if any job is failed, we fail also
      @jobs.each_pair do |name, job|
        case job.status
          when :completed
          when :failed
            raise "#{name}: #{job.get(:error)}"
          else
            suspend
        end
      end
    end

    # @param source SideJob::Port
    # @param targets [Array<Hash>] hash of format used by #get_port
    def connect_ports(source, targets)
      return if targets.size == 0

      if targets.size == 1
        # special case when there's only single input to send data to
        port = get_port(:in, targets[0])
        @to_restart << port.job if source.pop_all_to(port).length > 0
      else
        # copy the output to multiple ports
        loop do
          data = source.pop
          break unless data
          targets.each do |target|
            port = get_port(:in, target)
            port.push data
            @to_restart << port.job
          end
        end
      end
    end

    # @param type [:in, :out]
    # @param data [Hash] {'job' => '...', 'port' => '...'}
    # @return [SideJob::Port]
    def get_port(type, data)
      job = @jobs[data['job']]
      if type == :in
        job.input(data['port'])
      elsif type == :out
        job.output(data['port'])
      else
        nil
      end
    end
  end
end
