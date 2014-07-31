module SideJob
  # Input ports:
  #   in: Counts the number of data packets received on this port
  #   reset: Resets the count
  #   total: Expected total number of packets. If set, the progress will be stored in the job.
  # Output ports:
  #   count: Outputs the cumulative count
  class Count
    include SideJob::Worker
    def perform
      count = get(:count).to_i # nil.to_i => 0
      if input(:reset).size > 0
        input(:reset).clear
        count = 0
      end

      total = get(:total).to_i
      if input(:total).size > 0
        input(:total).trim(1)
        total = input(:total).pop.to_i
        set :total, total
      end

      inport = input(:in)
      loop do
        data = inport.pop
        break unless data
        count += 1
      end
      set :count, count

      if total > 0
        # store progress
        at(count, total)
      end
      output(:count).clear.push count
    end
  end
end
