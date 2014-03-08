module SideJob
  # Sends all data from :in to a differently named output port
  # Options:
  #   port: Name of output port to copy input data to
  # Input ports:
  #   in: Input port
  # Output ports:
  #   options['port']: everything received on :in
  class CopyTo
    include SideJob::Worker
    def perform(options)
      inport = input(:in)
      outport = output(options['port'])
      inport.pop_all_to(outport)
    end
  end
end
