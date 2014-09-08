class TestWorker
  include SideJob::Worker
  def perform(*args)
  end
end
