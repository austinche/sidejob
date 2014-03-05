# wait for a certain number of data signals on the inputs
# then sends a signal output
class TestWait
  include SideJob::Worker
  def perform(*args)
    count = get(:count)
    if ! count
      count = input('COUNT').pop
      suspend if ! count
      set(:count, count)
    end

    count = count.to_i
    loop do
      data = input('IN').pop
      break if ! data
      count -= 1
    end
    set(:count, count)
    if count <= 0
      output('READY').push '1'
    else
      suspend
    end
  end
end
