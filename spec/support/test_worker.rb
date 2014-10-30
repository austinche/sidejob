class TestWorker
  include SideJob::Worker
  register
  def perform(*args)
  end
end

class TestWorkerEmpty
  include SideJob::Worker
  register
  def perform
  end
end
