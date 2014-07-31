class TestSum
  include SideJob::Worker
  def perform
    suspend unless input(:ready).pop
    sum = 0
    while data = input(:in).pop
      sum += data.to_i
    end
    output(:sum).push sum.to_s
  end
end
