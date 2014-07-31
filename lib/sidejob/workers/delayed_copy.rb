module SideJob
  # Sends all data from :in to :out port with a configurable delay
  # This can be used to simulate process delays
  # Input ports:
  #   delay: delay in seconds
  #   in: input data
  # Output ports:
  #   out: Copy of input data
  class DelayedCopy
    include SideJob::Worker
    def perform
      delay = get_config(:delay).to_i
      inport = input(:in)
      outport = output(:out)
      loop do
        data = inport.pop
        break unless data
        sleep delay
        outport.push data
        notify
      end
    end
  end
end
