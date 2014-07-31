module SideJob
  # Runs a jq filter
  # Expects the jq program to be in the path
  # Input ports:
  #   filter: Filter in the jq language: http://stedolan.github.io/jq/manual/
  #   in: Input data
  # Output ports:
  #   out: Filter output
  class Filter
    include SideJob::Worker
    def perform
      filter = get_config(:filter)
      suspend unless filter

      IO.popen(['jq', '-c', filter], 'r+') do |io|
        # send data on input port to filter input
        inport = input(:in)
        loop do
          data = inport.pop
          break unless data
          io.puts data
        end
        io.close_write

        # send filter output to output port
        outport = output(:out)
        loop do
          data = io.gets
          break unless data
          data.chomp!
          outport.push data
        end
      end
    end
  end
end
