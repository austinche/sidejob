class TestWorker
  include SideJob::Worker
  register
  def perform(*args)
  end
end
