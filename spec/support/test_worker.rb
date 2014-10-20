class TestWorker
  include SideJob::Worker
  register(
      inports: {
          'memory' => { mode: :memory },
          'default' => { default: 'default' },
          'default_null' => { default: nil },
          'default_false' => { default: false},
          'memory_with_default' => { mode: :memory, default: 'memory default' },
          '*' => {},
      },
      outports: {
          '*' => {},
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
