class TestWorker
  include SideJob::Worker
  register(
      inports: {
          'memory' => { mode: :memory },
          'default' => { default: 'default' },
          '*' => {},
      },
      outports: {
          '*' => {},
      }
  )
  def perform
  end
end

class TestWorkerEmpty
  include SideJob::Worker
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
