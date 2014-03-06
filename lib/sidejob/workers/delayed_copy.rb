module SideJob
  # Sends all data from the IN port to the OUT port with a configurable delay
  # This can be used to simulate process delays
  # Input ports
  #   IN: input data
  #   DELAY: time in seconds (default 0)
  # Output ports
  #   OUT: Copy of input data
  class DelayedCopy
    include SideJob::Worker
    def perform(*args)
      inport = input('IN')
      outport = output('OUT')
      delay = get(:delay) || input('DELAY').pop
      delay = delay.to_i
      loop do
        data = inport.pop
        break unless data
        sleep delay
        outport.push data
      end
    end
  end
end
