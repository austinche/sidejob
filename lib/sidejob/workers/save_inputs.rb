module SideJob
  # Options:
  # Input ports:
  #   trigger: Sends all saved inputs to the same named output port in the received order
  #   All other ports: Data is read and saved
  # Output ports:
  #   Same name as input ports
  class SaveInputs
    include SideJob::Worker
    def perform(options={})
      inputs = get_json(:inputs) || {} # port name -> Array<String>
      trigger = input(:trigger)
      inports.each do |inport|
        next if inport == trigger
        inputs[inport.name] ||= []
        loop do
          data = inport.pop
          break unless data
          inputs[inport.name] << data if data
        end
      end
      set_json :inputs, inputs

      while trigger.pop
        inputs.each_pair do |name, data|
          output(name).push(*data)
        end
      end
    end
  end
end
