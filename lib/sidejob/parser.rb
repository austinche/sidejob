# Parser for a custom language to specify data flow between jobs

require 'parslet'

module SideJob
  class Parser < Parslet::Parser
    class Transform < Parslet::Transform
      # unescape strings
      rule(str: simple(:str)) do
        { str: str.to_s.gsub(/\\(.)/, "\\1") }
      end

      # handle job definitions
      rule(line: { job_definition: { job: simple(:job), queue: simple(:queue), class: simple(:job_class) }}) do
        { type: :job, job: job.to_s, queue: queue.to_s, class: job_class.to_s }
      end

      # handle connections
      rule(job: simple(:job), port: simple(:port)) { {job: job.to_s, port: port.to_s} }
      rule(job: simple(:job)) { job.to_s }
      rule(port: simple(:port)) { port.to_s }
      rule(line: { connection: { source: subtree(:source), tgt_job: simple(:tgt_job), tgt_port: simple(:tgt_port), next: subtree(:next_link), split: subtree(:split) }}) do
        last_source = source
        connections = [ {src: last_source, tgt: {job: tgt_job.to_s, port: tgt_port.to_s}} ]

        prev_job = tgt_job.to_s
        next_link.each do |port|
          connections << {src: {job: prev_job, port: port[:src_port].to_s}, tgt: {job: port[:tgt_job].to_s, port: port[:tgt_port].to_s}}
          last_source = {job: prev_job, port: port[:src_port].to_s}
          prev_job = port[:tgt_job].to_s
        end

        split.each do |other|
          connections << {src: last_source, tgt: {job: other[:job].to_s, port: other[:port].to_s}}
        end

        {type: :connections, connections: connections}
      end

      # lines
      rule(line: simple(:comment)) do
        # only lines to be ignored will match simple
        {type: :ignore}
      end

      # top level graph
      rule(graph: subtree(:graph)) do
        data = graph
        data = [ data ] if ! data.is_a?(Array) # if less than two items
        jobs = {}
        connections = []
        inports = {}
        outports = {}

        data.each do |line|
          case line[:type]
            when :ignore
              next
            when :job
              raise "#{line[:job]}: duplicate definition" if jobs[line[:job]]
              jobs[line[:job]] = { 'queue' => line[:queue], 'class' => line[:class] }
            when :connections
              # handle connections after all jobs
              connections.concat line[:connections]
            else
              raise "Unable to transform line: #{line}"
          end
        end

        connections.each do |conn|
          src = conn[:src]
          tgt = conn[:tgt]
          if src[:job] == '@'
            # graph inport
            inports[src[:port]] ||= []
            inports[src[:port]] << {'job' => tgt[:job], 'port' => tgt[:port]}
          elsif tgt[:job] == '@'
            # graph outport
            outports[tgt[:port]] ||= []
            outports[tgt[:port]] << {'job' => src[:job], 'port' => src[:port]}
          else
            raise "Undefined job #{tgt[:job]}" unless jobs[tgt[:job]]

            if src[:str]
              jobs[tgt[:job]]['init'] ||= {}
              jobs[tgt[:job]]['init'][tgt[:port]] ||= []
              jobs[tgt[:job]]['init'][tgt[:port]] << src[:str]
            else
              raise "Undefined job #{src[:job]}" unless jobs[src[:job]]
              jobs[src[:job]]['connections'] ||= {}
              jobs[src[:job]]['connections'][src[:port]] ||= []
              jobs[src[:job]]['connections'][src[:port]] << {'job' => tgt[:job], 'port' => tgt[:port]}
            end
          end
        end

        res = {'jobs' => jobs}
        res['inports'] = inports if inports.size > 0
        res['outports'] = outports if outports.size > 0
        res
      end

    end

    rule(:space) { match[' \t'] }
    rule(:whitespace) { space.repeat }
    rule(:newline) { match['\n\r'] }
    rule(:space_or_newline) { match[' \t\n\r'] }
    rule(:safe_name) { match['a-zA-Z0-9_'].repeat(1) }
    rule(:queue_name) { safe_name }
    rule(:class_name) { (safe_name | match[':.']).repeat(1) }
    rule(:job_name) { str('[') >> safe_name.as(:job) >> str(']') }
    rule(:job_name_or_self) { str('@').as(:job) | job_name }
    rule(:port_name) { safe_name.as(:port) }
    rule(:inport) { port_name >> str(':') }
    rule(:outport) { str(':') >> port_name }

    rule(:quote) { str("'") }
    rule(:string) { quote >> ((str('\\') | quote.absent?) >> any).repeat.as(:str) >> quote }

    rule(:connection_source) { job_name_or_self >> outport }
    rule(:connection_target) { whitespace >> str('->') >> whitespace >> inport.as(:tgt_port) >> job_name_or_self.as(:tgt_job) }
    rule(:connection_next) { outport.as(:src_port) >> whitespace >> connection_target }
    rule(:connection_split) { whitespace >> str('+') >> whitespace >> inport >> job_name_or_self }
    # string/init data only allowed at beginning
    # split connection only allowed at end of chain
    rule(:connections) { (string | connection_source).as(:source) >> connection_target >> connection_next.repeat.as(:next) >> connection_split.repeat.as(:split) }

    rule(:job_definition) { job_name >> whitespace >> str('=') >> whitespace >> queue_name.as(:queue) >> whitespace >> class_name.as(:class) }

    rule(:comment) { str('#') >> (newline.absent? >> any).repeat }

    rule(:line) { whitespace >> (job_definition.as(:job_definition) | connections.as(:connection)).maybe >> whitespace >> comment.maybe }
    rule(:graph) { ((line.as(:line) >> newline).repeat >> line.maybe.as(:line)).as(:graph) } # last line may not end with line terminator

    root(:graph)

    # Entry point for parsing a flow graph into something manageable
    # @param str [String]
    # @return [Hash]
    def self.parse(str)
      parsed = SideJob::Parser.new.parse(str)
      Transform.new.apply(parsed)
    rescue Parslet::ParseFailed => error
      raise error.cause.ascii_tree
    end

    # Converts output of parse back into the flow graph language
    # @param graph [Hash]
    # @return [String]
    def self.unparse(graph)
      lines = []
      graph['jobs'].each_pair do |name, info|
        lines << "[#{name}] = #{info['queue']} #{info['class']}"

        if info['connections']
          info['connections'].each_pair do |outport, targets|
            all_targets = []
            targets.each do |target|
              all_targets << "#{target['port']}:[#{target['job']}]"
            end
            lines << "[#{name}]:#{outport} -> #{all_targets.join(' + ')}"
          end
        end

        if info['init']
          info['init'].each_pair do |port, data|
            data.each do |x|
              lines << "'#{x.gsub("'") {|x| "\\'"}}' -> #{port}:[#{name}]"
            end
          end
        end
      end

      if graph['inports']
        graph['inports'].each_pair do |port, targets|
          targets.each do |target|
            lines << "@:#{port} -> #{target['port']}:[#{target['job']}]"
          end
        end
      end

      if graph['outports']
        graph['outports'].each_pair do |port, sources|
          sources.each do |source|
            lines << "[#{source['job']}]:#{source['port']} -> #{port}:@"
          end
        end
      end

      lines.join("\n")
    end
  end
end
