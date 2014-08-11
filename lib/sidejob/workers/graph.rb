module SideJob
  # Input ports:
  #   graph: flow graph in noflo graph json format https://github.com/noflo/noflo/blob/master/graph-schema.json
  #   Ports specified in graph
  # Output ports
  #   Ports specified in graph
  class Graph
    include SideJob::Worker

    def perform
      graph = get_config_json(:graph)
      suspend unless graph

      @jobs = {} # cache SideJob::Job objects by job name
      @to_restart = Set.new

      # make sure all jobs are started
      graph['processes'].each_pair do |name, info|
        info['metadata'] ||= {}
        @jobs[name] = SideJob.find(info['metadata']['jid']) if info['metadata']['jid']
        if ! @jobs[name]
          # start a new job
          # component name must be of form queue/ClassName
          queue, klass = info['component'].split('/', 2)
          raise "Unable to parse #{info['component']}: Must be of form queue/ClassName" if ! queue || ! klass
          job = queue(queue, klass)
          info['metadata']['jid'] = job.jid

          set_json :graph, graph

          job.set(:name, name)
          @jobs[name] = job
        end
      end

      connections = {} # SideJob::Port (output port) -> Array<SideJob::Port> (input ports)
      graph['connections'].each do |connection|
        tgt_port = get_port(:in, connection['tgt'])

        if connection['data']
          # initial fixed data to be sent only once
          connection['metadata'] ||= {}
          if ! connection['metadata']['sent']
            tgt_port.push connection['data']
            connection['metadata']['sent'] = true
            set_json :graph, graph
          end
        else
          src_port = get_port(:out, connection['src'])

          connections[src_port] ||= []
          connections[src_port] << tgt_port
        end
      end

      # outport connections have to be merged with job connections in case
      # some data needs to go to both another job and a graph outport
      if graph['outports']
        graph['outports'].each_pair do |name, port|
          out = get_port(:out, port)
          connections[out] ||= []
          connections[out] << output(name)
        end
      end

      # process all connections

      if graph['inports']
        graph['inports'].each_pair do |name, port|
          connect_ports(input(name), [get_port(:in, port)])
        end
      end

      connections.each_pair do |port, targets|
        connect_ports(port, targets)
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
    # @param targets [Array<SideJob::Port>]
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
    # @param data [Hash, Port] {'process' => '...', 'port' => '...'}. If Port given, just returns it
    # @return [SideJob::Port]
    def get_port(type, data)
      return data if data.is_a?(SideJob::Port)
      job = @jobs[data['process']]
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
