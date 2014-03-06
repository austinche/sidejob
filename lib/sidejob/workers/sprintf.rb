module SideJob
  # Input ports
  #   FORMAT: Format string in the format of Ruby's Kernel#sprintf with named references
  #   Any other port is used for substitution
  #   Only the last data received on any port is used
  # Output ports
  #   OUT: Outputs the formatted string
  class Sprintf
    include SideJob::Worker
    def perform(*args)
      references = get_json(:references) || {}

      format_port = input('FORMAT')
      format = format_port.pop
      format ||= get(:format)
      inports.each do |inport|
        next if inport == format_port
        inport.trim(1) # get rid of everything but the last data
        data = inport.pop
        if data
          references[inport.name] = data
        end
      end

      set :format, format
      set_json :references, references

      if format
        begin
          # symbolize keys for this to work
          references = Hash[references.map{ |k, v| [k.to_sym, v] }]
          formatted = (format % references)
          output('OUT').push formatted
        rescue KeyError
          # assume missing input if we get a KeyError
          suspend
        end
      end
    end
  end
end
