module SideJob
  # Options:
  #   format: Format string in the format of Ruby's Kernel#sprintf with named references
  # Input ports:
  #   The port names should match the reference name in the format string
  #   Only the last data received on any port is used
  # Output ports:
  #   out: Outputs the formatted string
  class Sprintf
    include SideJob::Worker
    def perform(options={})
      references = get_json(:references) || {}

      inports.each do |inport|
        inport.trim(1) # get rid of everything but the last data
        data = inport.pop
        if data
          references[inport.name] = data
        end
      end

      set_json :references, references

      begin
        # symbolize keys for this to work
        references = Hash[references.map{ |k, v| [k.to_sym, v] }]
        formatted = (options['format'] % references)
        output(:out).push formatted
      rescue KeyError
        # assume missing input if we get a KeyError
        suspend
      end
    end
  end
end
