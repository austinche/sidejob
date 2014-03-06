module SideJob
  # Input ports
  #   IN: Counts the number of data packets received on this port
  #   RESET: Resets the count
  #   TOTAL: Expected total number of packets. If set, the progress will be stored in the job.
  # Output ports
  #   COUNT: Outputs the cumulative count
  class Count
    include SideJob::Worker
    def perform(*args)
      count = get(:count).to_i # nil.to_i => 0
      if input('RESET').size > 0
        input('RESET').clear
        count = 0
      end

      total = get(:total).to_i
      if input('TOTAL').size > 0
        input('TOTAL').trim(1)
        total = input('TOTAL').pop.to_i
        set :total, total
      end

      inport = input('IN')
      loop do
        data = inport.pop
        break unless data
        count += 1
        if total > 0
          # store progress
          at(count, total)
        end
      end
      set :count, count
      output('COUNT').clear.push count
    end
  end
end
