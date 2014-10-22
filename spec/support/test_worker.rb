class TestWorker
  include SideJob::Worker
  register(
      inports: {
          static: {}
      },
      outports: {
          static: {}
      }
  )
  def perform(*args)
  end
end

class TestWorkerEmpty
  include SideJob::Worker
  register
  def perform
  end
end

class TestWorkerNoLog
  include SideJob::Worker
  register(
      worker: { log_status: false }
  )
  def perform
  end
end
