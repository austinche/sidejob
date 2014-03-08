# wait for a certain number of data signals on the inputs
# then sends a signal output
class TestWait
  include SideJob::Worker
  def perform(config)
    total = config['total'].to_i
    count = get(:count).to_i

    loop do
      data = input('IN').pop
      break if ! data
      count += 1
    end
    set(:count, count)
    if count >= total
      output('READY').push '1'
    else
      suspend
    end
  end
end
