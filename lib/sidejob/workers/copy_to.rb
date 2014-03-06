module SideJob
  # Sends all data from the IN port to a differently named output port
  # Input ports
  #   PORT: Name of the output port
  #   IN: Input port
  # Output ports
  #   Outputs to port PORT everything received on IN
  class CopyTo
    include SideJob::Worker
    def perform(*args)
      port = get(:port)
      if ! port
        port = input('PORT').pop
        set(:port, port) if port
      end

      suspend unless port

      inport = input('IN')
      outport = output(port)
      inport.pop_all_to(outport)
    end
  end
end
