# wait for a certain number of data signals on the inputs
# then sends a signal output
class TestWait
  include SideJob::Worker
  def perform
    total = get_config(:total)
    suspend and return unless total
    total = total.to_i
    count = get(:count).to_i

    loop do
      data = input(:in).pop
      break if ! data
      count += 1
    end
    set(:count, count)
    if count >= total
      output(:ready).push '1'
    else
      suspend
    end
  end
end
