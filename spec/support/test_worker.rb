class TestWorker
  include SideJob::Worker
  def perform
  end
end

class TestWorkerNoLog
  include SideJob::Worker
  register(
      run: { log_status: false }
  )
  def perform
  end
end

class TestWorkerMemory
  include SideJob::Worker
  register(
      inports: {
          memory: { mode: :memory }
      }
  )
  def perform
  end
end
