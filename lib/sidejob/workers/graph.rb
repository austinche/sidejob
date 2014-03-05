# Execute flow graphs written in the the flow based programming (FBP) language used by noflo
# https://github.com/noflo/fbp

require 'execjs'
module SideJob
  class Graph
    include SideJob::Worker

    # Input and output ports must be explicitly exported in the graph via INPORT and OUTPORT
    # @param graph [String] graph in the flow based programming (FBP) language
    def perform(graph_fbp, *args)
      graph = get(:graph)
      if graph
        graph = JSON.parse(graph)
      else
        graph = fbp_compile(graph_fbp)
      end

      # we store extra info in the graph hash beyond what fbp uses
      jobs = {}
      graph['processes'].each_pair do |name, info|
        if ! info['jid']
          # start a new job
          # component name must be of form queue/ClassName
          # currently there's no easy way to pass job arguments via the fbp language
          queue, klass = info['component'].split('/', 2)
          raise "Unable to parse #{info['component']}: Must be of form queue/ClassName" if ! queue || ! klass
          info['jid'] = queue(queue, klass)
        end
        jobs[name] = SideJob::Job.new(info['jid'])
      end

      to_restart = Set.new

      connections = {}
      graph['connections'].each do |connection|
        if connection['data']
          # initial fixed data to be sent only once
          if ! connection['done']
            target = connection['tgt']
            job = jobs[target['process']]
            job.input(target['port']).push connection['data']
            connection['done'] = true
          end
        else
          src = connection['src']
          src_job = jobs[src['process']]
          src_port = src_job.output(src['port'])

          connections[src_port] ||= []

          tgt = connection['tgt']
          tgt_job = jobs[tgt['process']]
          tgt_port = tgt_job.input(tgt['port'])

          connections[src_port] << tgt_port
        end
      end

      connections.each_pair do |src, targets|
        if targets.length == 1
          if src.pop_all_to(targets[0]).length > 0
            to_restart << targets[0].job
          end
        else
          # we have to copy the output to multiple ports
          # make this operation atomic with a transaction
          data = src.pop
          if data
            targets.each do |target|
              target.push data
              to_restart << target.job
            end
          end
        end
      end

      if graph['inports']
        graph['inports'].each_pair do |port, to|
          target_job = jobs[to['process']]
          target_port = target_job.input(to['port'])
          data = input(port).pop_all_to(target_port)
          if data.length > 0
            to_restart << target_job
          end
        end
      end

      if graph['outports']
        graph['outports'].each_pair do |port, from|
          src_job = jobs[from['process']]
          src_port = src_job.output(from['port'])
          src_port.pop_all_to(output(port))
        end
      end

      set(:graph, JSON.generate(graph))

      to_restart.each do |job|
        job.restart
      end

      # we complete if all jobs are completed
      # if any job is failed, we fail also
      jobs.each_pair do |name, job|
        case job.status
          when :completed
          when :failed
            raise "#{name}: #{job.get(:error)}"
          else
            suspend
        end
      end
    end

    def fbp_compile(graph)
      compiler = ExecJS.compile("module = {};" + File.read(File.expand_path('../../../../node_modules/fbp/lib/fbp.js', __FILE__)))
      return compiler.call("module.exports.parse", graph)
    end

  end
end
