class TestWorker
  include SideJob::Worker
  def perform(*args)
  end
end

class TestWorkerNoLog
  include SideJob::Worker
  configure log_status: false
  def perform(*args)
  end
end
