module SideJob
  # Resends all data from :in to :out port when triggered
  # Input ports:
  #   trigger: Any data on this port triggers sending to output port
  #   in: Input data
  # Output ports:
  #   out: Sends all data received on input when triggered
  class Repeat
    include SideJob::Worker
    def perform
      inputs = get_json(:inputs) || [] # received inputs
      trigger = input(:trigger)
      inport = input(:in)
      loop do
        data = inport.pop
        break unless data
        inputs << data
      end
      set_json :inputs, inputs

      outport = output(:out)
      while trigger.pop
        outport.push(*inputs)
      end
    end
  end
end
