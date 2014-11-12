require 'spec_helper'

describe SideJob::ServerMiddleware do
  class TestWorkerShutdown
    include SideJob::Worker
    register
    attr_accessor :shutdown_called
    def shutdown
      @shutdown_called = true
    end
    def perform
    end
  end

  class TestWorkerShutdownError
    include SideJob::Worker
    register
    def shutdown
      raise 'shutdown error'
    end
    def perform
    end
  end

  before do
    @queue = 'testq'
    @job = SideJob.queue(@queue, 'TestWorker')
  end

  def process(job)
    chain = Sidekiq::Middleware::Chain.new
    chain.add SideJob::ServerMiddleware
    msg = Sidekiq::Queue.new(@queue).find_job(job.id)
    worker = msg.klass.constantize.new
    worker.jid = job.id
    chain.invoke(worker, msg, @queue) { yield worker }
    job.reload
    worker
  end

  %w{running suspended completed failed terminated}.each do |status|
    it "does not run if status is #{status}" do
      @job.status = status
      @run = false
      process(@job) { @run = true}
      expect(@run).to be false
      expect(@job.status).to eq status
    end
  end

  it 'does not run if job has been deleted' do
    @job.status = 'terminated'
    @job.delete
    @run = false
    process(@job) { @run = true}
    expect(@run).to be false
  end

  describe 'handles a normal run' do
    it 'sets status to running on start and completed on completion' do
      process(@job) { |worker| @status = worker.status }
      expect(@status).to eq 'running'
      expect(@job.status).to eq 'completed'
    end

    it 'runs the parent job' do
      @job.status = 'suspended'
      child = SideJob.queue(@queue, 'TestWorker', parent: @job, name: 'child')
      expect(@job.status).to eq 'suspended'
      expect {
        child.run_inline
      }.to change {Sidekiq::Stats.new.enqueued}.by(1)
      @job.reload
      expect(@job.status).to eq 'queued'
    end

    it 'sets the ran_at time at the beginning of the run' do
      now = Time.now
      allow(Time).to receive(:now) { now }
      process(@job) { |worker| @ran_at = worker.get(:ran_at) }
      expect(@ran_at).to eq SideJob.timestamp
      expect(@job.status).to eq 'completed'
    end
  end

  describe 'prevents multiple threads running the same job' do
    it 'sets the job lock to the current time' do
      now = Time.now
      allow(Time).to receive(:now) { now }
      process(@job) { @lock = SideJob.redis.get("#{@job.redis_key}:lock").to_f }
      expect(@lock).to eq(now.to_f)
      expect(SideJob.redis.exists("#{@job.redis_key}:lock")).to be false
    end

    it 'sets the job lock to the current time and does not run if already locked' do
      now = Time.now
      allow(Time).to receive(:now) { now }
      @run = false
      SideJob.redis.set "#{@job.redis_key}:lock", (now-10).to_f
      process(@job) { @run = true }
      expect(@run).to be false
      expect(SideJob.redis.get("#{@job.redis_key}:lock").to_f).to eq now.to_f
    end

    it 'does not restart the worker unless another worker was locked out during the run' do
      expect {
        process(@job) {}
      }.to change {Sidekiq::Stats.new.enqueued}.by(0)
      expect(@job.status).to eq 'completed'
    end

    it 'restarts the worker if another worker was locked out during the run' do
      expect {
        process(@job) { SideJob.redis.set "#{@job.redis_key}:lock", Time.now.to_f }
      }.to change {Sidekiq::Stats.new.enqueued}.by(1)
      expect(@job.status).to eq 'queued'
    end
  end

  describe 'prevents job loops' do
    it 'does not run if called too many times in a minute' do
      now = Time.now
      allow(Time).to receive(:now) { now }
      key = "#{@job.redis_key}:rate:#{Time.now.to_i/60}"
      SideJob.redis.set key, SideJob::ServerMiddleware::CONFIGURATION[:max_runs_per_minute]
      @run = false
      process(@job) { @run = true }
      expect(@run).to be false
      expect(@job.status).to eq 'terminating'
    end

    it 'does run if not called too many times in a minute' do
      now = Time.now
      allow(Time).to receive(:now) { now }
      key = "#{@job.redis_key}:rate:#{Time.now.to_i/60}"
      SideJob.redis.set key, SideJob::ServerMiddleware::CONFIGURATION[:max_runs_per_minute]-1
      @run = false
      process(@job) { @run = true }
      expect(@run).to be true
      expect(@job.status).to eq 'completed'
    end

    it 'does not run if job is too deep' do
      (SideJob::ServerMiddleware::CONFIGURATION[:max_depth]+1).times do |i|
        @job = SideJob.queue(@queue, 'TestWorker', parent: @job, name: 'child')
      end
      @run = false
      process(@job) { @run = true }
      expect(@run).to be false
      expect(@job.status).to eq 'terminating'
    end

    it 'does run if job is not too deep' do
      SideJob::ServerMiddleware::CONFIGURATION[:max_depth].times do |i|
        @job = SideJob.queue(@queue, 'TestWorker', parent: @job, name: 'child')
      end
      @run = false
      process(@job) { @run = true }
      expect(@run).to be true
      expect(@job.status).to eq 'completed'
    end
  end

  describe 'handles worker exceptions' do
    it 'sets status to failed on exception and logs error' do
      now = Time.now
      allow(Time).to receive(:now) { now }
      process(@job) { raise 'oops' }
      expect(@job.status).to eq 'failed'

      log = SideJob.logs.select {|log| log['error'] }
      expect(log.size).to eq(1)
      expect(log[0]['job']).to eq @job.id
      expect(log[0]['error']).to eq('oops')
      # check that we trim down backtrace to remove sidekiq lines
      expect(log[0]['backtrace']).to_not match(/sidekiq/)
    end

    it 'does not set status to failed if status is not running' do
      process(@job) do |worker|
        worker.run
        raise 'oops'
      end
      expect(@job.status).to eq 'queued'
    end

    it 'runs the parent job' do
      @job.status = 'suspended'
      child = SideJob.queue(@queue, 'TestWorker', parent: @job, name: 'child')
      expect {
        process(child) { raise 'oops' }
      }.to change {Sidekiq::Stats.new.enqueued}.by(1)
      @job.reload
      expect(@job.status).to eq 'queued'
    end
  end

  describe 'handles worker suspend' do
    it 'sets status to suspended' do
      process(@job) { |worker| worker.suspend }
      expect(@job.status).to eq 'suspended'
    end

    it 'does not set status to suspended if job was requeued' do
      process(@job) do |worker|
        worker.run
        worker.suspend
      end
      expect(@job.status).to eq 'queued'
    end
  end

  describe 'handles job termination' do
    it 'sets status to terminated upon run' do
      @job.status = 'terminating'
      process(@job) { raise 'should not be called' }
      expect(@job.status).to eq 'terminated'
      errors = SideJob.logs.select {|log| log['type'] == 'error'}
      expect(errors.size).to eq 0
    end

    it 'runs parent' do
      child = SideJob.queue(@queue, 'TestWorker', parent: @job, name: 'child')
      child.status = 'terminating'
      process(child) { raise 'should not be called' }
      expect(child.status).to eq 'terminated'
      expect(@job.status).to eq 'queued'
    end

    it 'calls worker shutdown method' do
      @job = SideJob.queue(@queue, 'TestWorkerShutdown')
      @job.status = 'terminating'
      worker = process(@job) { raise 'not reached' }
      expect(worker.shutdown_called).to be true
    end

    it 'logs but ignores exceptions thrown during shutdown' do
      @job = SideJob.queue(@queue, 'TestWorkerShutdownError')
      @job.status = 'terminating'
      worker = process(@job) { raise 'not reached' }
      logs = SideJob.logs.select {|log| log['error']}
      expect(logs.size).to eq 1
      expect(logs[0]['job']).to eq @job.id
      expect(logs[0]['error']).to eq 'shutdown error'
    end
  end
end
