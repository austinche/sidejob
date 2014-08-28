class TestSum
  include SideJob::Worker
  def perform
    suspend and return unless input(:ready).read
    sum = input(:in).drain.map {|x| x.to_i}.inject(&:+)
    output(:sum).write sum.to_s
  end
end
