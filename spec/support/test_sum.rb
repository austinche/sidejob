class TestSum
  include SideJob::Worker
  def perform
    suspend unless input(:ready).read
    sum = input(:in).drain.map {|x| x}.inject(&:+)
    output(:sum).write sum
  end
end
