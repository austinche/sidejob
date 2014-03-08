module SideJob
  # Sends all data from :in to :out port with a configurable delay
  # This can be used to simulate process delays
  # Options:
  #   delay: delay in seconds
  # Input ports:
  #   in: input data
  # Output ports:
  #   out: Copy of input data
  class DelayedCopy
    include SideJob::Worker
    def perform(options={})
      delay = options['delay'].to_i
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
