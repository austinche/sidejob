class TestSum
  include SideJob::Worker
  def perform(*args)
    suspend unless input('READY').pop
    sum = 0
    while data = input('IN').pop
      sum += data.to_i
    end
    output('SUM').push sum.to_s
  end
end
