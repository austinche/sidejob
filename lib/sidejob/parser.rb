# Parser for a custom language to specify data flow between jobs

require 'parslet'

module SideJob
  class Parser < Parslet::Parser
    class Transform < Parslet::Transform
      # handle job definitions
      rule(name: simple(:name), value: simple(:value)) { [name.to_s, value.to_s] }
      rule(line: { job_definition: { job: simple(:job), queue: simple(:queue), class: simple(:job_class), args: subtree(:args) }}) do
        merged = args.inject({}) do |h, arg|
          key = arg[0]
          val = arg[1]
          raise "#{job}: argument #{key} duplicated" if h[key]
          h[key.to_s] = val
          h
        end
        { type: :job, job: job.to_s, queue: queue.to_s, class: job_class.to_s, args: merged }
      end

      # handle connections
      rule(job: simple(:job)) { job }
      rule(port: simple(:port)) { port }
      rule(line: { connection: { src_job: simple(:src_job), src_port: simple(:src_port), tgt_job: simple(:tgt_job), tgt_port: simple(:tgt_port), next: subtree(:next_link), split: subtree(:split) }}) do
        connections = [ {src: {job: src_job.to_s, port: src_port.to_s}, tgt: {job: tgt_job.to_s, port: tgt_port.to_s}} ]
        prev_job = tgt_job.to_s
        last_outport = [src_job.to_s, src_port.to_s]
        next_link.each do |port|
          connections << {src: {job: prev_job, port: port[:src_port].to_s}, tgt: {job: port[:tgt_job].to_s, port: port[:tgt_port].to_s}}
          last_outport = [prev_job, port[:src_port].to_s]
          prev_job = port[:tgt_job].to_s
        end

        split.each do |other|
          connections << {src: {job: last_outport[0], port: last_outport[1]}, tgt: {job: other[:job].to_s, port: other[:port].to_s}}
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
              if line[:args] && line[:args].size > 0
                jobs[line[:job]]['args'] = line[:args]
              end
            when :connections
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
            raise "Undefined job #{src[:job]}" unless jobs[src[:job]]
            raise "Undefined job #{tgt[:job]}" unless jobs[tgt[:job]]
            jobs[src[:job]]['connections'] ||= {}
            jobs[src[:job]]['connections'][src[:port]] ||= []
            jobs[src[:job]]['connections'][src[:port]] << {'job' => tgt[:job], 'port' => tgt[:port]}
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
    rule(:job_name_or_self) { str('@') | job_name }
    rule(:port_name) { safe_name.as(:port) }
    rule(:argument_name) { safe_name }
    rule(:inport) { port_name >> str(':') }
    rule(:outport) { str(':') >> port_name }

    rule(:connection) { outport.as(:src_port) >> whitespace >> str('->') >> whitespace >> inport.as(:tgt_port) >> job_name_or_self.as(:tgt_job) }
    rule(:connection_chain) { connection.repeat }
    # split connection only allowed at end of chain
    rule(:connection_split) { whitespace >> str('+') >> whitespace >> inport >> job_name_or_self }
    rule(:connections) { job_name_or_self.as(:src_job) >> connection >> connection.repeat.as(:next) >> connection_split.repeat.as(:split) }

    rule(:matching_quote) { dynamic {|s, c| str(c.captures[:quote])} }
    rule(:quoted_string) { match['\'"'].capture(:quote) >> ((matching_quote.absent? >> any).repeat).as(:value) >> matching_quote }
    rule(:unquoted_string) { (str("'") | str('"')).absent? >> (space_or_newline.absent? >> any).repeat(1) }
    rule(:string) { unquoted_string.as(:value) | quoted_string }
    rule(:job_argument) { whitespace >> argument_name.as(:name) >> str(':') >> string }
    rule(:job_definition) { job_name >> whitespace >> str('=') >> whitespace >> queue_name.as(:queue) >> whitespace >> class_name.as(:class) >> job_argument.repeat.as(:args) }

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
  end
end
