class TestSum
  include SideJob::Worker
  def perform
    suspend unless input(:ready).data?
    sum = input(:in).inject(&:+)
    output(:sum).write sum
  end
end
