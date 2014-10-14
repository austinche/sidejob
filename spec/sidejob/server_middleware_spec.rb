require 'spec_helper'

describe SideJob::ServerMiddleware do
  before do
    @queue = 'testq'
    @job = SideJob.queue(@queue, 'TestWorker')
  end

  def process(job)
    sjob = Sidekiq::Queue.new(@queue).find_job(job.jid)
    worker = sjob.klass.constantize.new
    worker.jid = sjob.jid
    chain = Sidekiq::Middleware::Chain.new
    chain.add SideJob::ServerMiddleware
    chain.invoke(worker, sjob.item, @queue) { yield worker }
  end

  %w{running suspended completed failed terminated}.each do |status|
    it "does not run if status is #{status}" do
      set_status @job, status
      @run = false
      process(@job) { @run = true}
      expect(@run).to be false
      expect(@job.status).to eq status
    end
  end

  it 'does not run if job has been deleted' do
    SideJob.redis.hset @job.redis_key, 'status', 'terminated'
    @job.delete
    @run = false
    process(@job) { @run = true}
    expect(@run).to be false
  end

  describe 'handles a normal run' do
    it 'sets status to running on start and completed on completion' do
      process(@job) { @status = @job.status }
      expect(@status).to eq 'running'
      expect(@job.status).to eq 'completed'
    end

    it 'logs running and completed status' do
      process(@job) { }
      logs = @job.logs.select {|log| log['type'] == 'status'}
      expect(logs[0]['status']).to eq 'completed'
      expect(logs[1]['status']).to eq 'running'
    end

    it 'does not log status if configured to not log' do
      @job = SideJob.queue(@queue, 'TestWorkerNoLog')
      process(@job) { }
      expect(@job.logs.select {|log| log['type'] == 'status'}.size).to be 0
    end

    it 'runs the parent job' do
      SideJob.redis.hset @job.redis_key, 'status', 'suspended'
      child = SideJob.queue(@queue, 'TestWorker', {parent: @job})
      expect(@job.status).to eq 'suspended'
      expect {
        process(child) {}
      }.to change {Sidekiq::Stats.new.enqueued}.by(1)
      expect(@job.status).to eq 'queued'
    end

    it 'sets the ran_at time at the beginning of the run' do
      now = Time.now
      Time.stub(:now).and_return(now)
      process(@job) { @ran_at = SideJob.redis.hget(@job.redis_key, 'ran_at') }
      expect(@ran_at).to eq SideJob.timestamp
      expect(@job.status).to eq 'completed'
    end
  end

  describe 'prevents multiple threads running the same job' do
    it 'sets the job lock to the current time' do
      now = Time.now
      Time.stub(:now).and_return(now)
      process(@job) { @lock = SideJob.redis.get("#{@job.redis_key}:lock").to_f }
      expect(@lock).to eq(now.to_f)
      expect(SideJob.redis.exists("#{@job.redis_key}:lock")).to be false
    end

    it 'sets the job lock to the current time and does not run if already locked' do
      now = Time.now
      Time.stub(:now).and_return(now)
      @run = false
      SideJob.redis.set "#{@job.redis_key}:lock", (now-10).to_f
      process(@job) { @run = true }
      expect(@run).to be false
      expect(SideJob.redis.get("#{@job.redis_key}:lock").to_f).to eq now.to_f
    end

    it 'does not do anything if the enqueued_at time is before the ran_at' do
      @run = false
      SideJob.redis.hset @job.redis_key, 'ran_at', SideJob.timestamp
      process(@job) {@run = true}
      expect(@run).to be false
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
      Time.stub(:now).and_return(now)
      key = "#{@job.redis_key}:rate:#{Time.now.to_i/60}"
      SideJob.redis.set key, SideJob::ServerMiddleware::DEFAULT_CONFIGURATION['max_runs_per_minute']
      @run = false
      process(@job) { @run = true }
      expect(@run).to be false
      expect(@job.status).to eq 'terminated'
    end

    it 'does run if not called too many times in a minute' do
      now = Time.now
      Time.stub(:now).and_return(now)
      key = "#{@job.redis_key}:rate:#{Time.now.to_i/60}"
      SideJob.redis.set key, SideJob::ServerMiddleware::DEFAULT_CONFIGURATION['max_runs_per_minute']-1
      @run = false
      process(@job) { @run = true }
      expect(@run).to be true
      expect(@job.status).to eq 'completed'
    end

    it 'does not run if job is too deep' do
      (SideJob::ServerMiddleware::DEFAULT_CONFIGURATION['max_depth']+1).times do |i|
        @job = SideJob.queue(@queue, 'TestWorker', {parent: @job})
      end
      @run = false
      process(@job) { @run = true }
      expect(@run).to be false
      expect(@job.status).to eq 'terminated'
    end

    it 'does run if job is not too deep' do
      SideJob::ServerMiddleware::DEFAULT_CONFIGURATION['max_depth'].times do |i|
        @job = SideJob.queue(@queue, 'TestWorker', {parent: @job})
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
      Time.stub(:now).and_return(now)
      process(@job) { raise 'oops' }
      expect(@job.status).to eq 'failed'

      log = @job.logs.select {|log| log['type'] == 'error'}
      expect(log.size).to eq(1)
      expect(log[0]['error']).to eq('oops')
      # check that we trim down backtrace to remove sidekiq lines
      expect(log[0]['backtrace']).to_not match(/sidekiq/)
    end

    it 'logs failed status change' do
      process(@job) { raise 'oops' }
      log = @job.logs.detect {|log| log['type'] == 'status'}
      expect(log['status']).to eq 'failed'
    end

    it 'does not log if configured to not log' do
      @job = SideJob.queue(@queue, 'TestWorkerNoLog')
      process(@job) { raise 'oops' }
      expect(@job.logs.select {|log| log['type'] == 'status'}.size).to be 0
    end

    it 'does not set status to failed if status is not running' do
      process(@job) do
        @job.run
        raise 'oops'
      end
      expect(@job.status).to eq 'queued'
    end

    it 'runs the parent job' do
      SideJob.redis.hset @job.redis_key, 'status', 'suspended'
      child = SideJob.queue(@queue, 'TestWorker', {parent: @job})
      expect {
        process(child) { raise 'oops' }
      }.to change {Sidekiq::Stats.new.enqueued}.by(1)
      expect(@job.status).to eq 'queued'
    end
  end

  describe 'handles worker suspend' do
    it 'sets status to suspended' do
      process(@job) {|worker| worker.suspend}
      expect(@job.status).to eq 'suspended'
    end

    it 'logs suspended status change' do
      process(@job) {|worker| worker.suspend}
      log = @job.logs.detect {|log| log['type'] == 'status'}
      expect(log['status']).to eq 'suspended'
    end

    it 'does not log if configured to not log' do
      @job = SideJob.queue(@queue, 'TestWorkerNoLog')
      process(@job) {|worker| worker.suspend}
      expect(@job.logs.select {|log| log['type'] == 'status'}.size).to be 0
    end

    it 'does not set status to suspended if job was requeued' do
      process(@job) do |worker|
        @job.run
        worker.suspend
      end
      expect(@job.status).to eq 'queued'
    end
  end

  describe 'handles job termination' do
    it 'sets status to terminated upon run' do
      SideJob.redis.hset @job.redis_key, 'status', 'terminating'
      process(@job) { raise 'should not be called' }
      expect(@job.status).to eq 'terminated'
      errors = @job.logs.select {|log| log['type'] == 'error'}
      expect(errors.size).to eq 0
    end

    it 'logs terminated status change' do
      SideJob.redis.hset @job.redis_key, 'status', 'terminating'
      process(@job) { raise 'should not be called' }
      log = @job.logs.detect {|log| log['type'] == 'status'}
      expect(log['status']).to eq 'terminated'
    end

    it 'does not log if configured to not log' do
      @job = SideJob.queue(@queue, 'TestWorkerNoLog')
      SideJob.redis.hset @job.redis_key, 'status', 'terminating'
      process(@job) { raise 'should not be called' }
      expect(@job.logs.select {|log| log['type'] == 'status'}.size).to be 0
    end

    it 'runs parent' do
      child = SideJob.queue(@queue, 'TestWorker', {parent: @job})
      SideJob.redis.hset child.redis_key, 'status', 'terminating'
      process(child) { raise 'should not be called' }
      expect(child.status).to eq 'terminated'
      expect(@job.status).to eq 'queued'
    end

    it 'calls worker shutdown method' do
      SideJob.redis.hset @job.redis_key, 'status', 'terminating'
      sjob = Sidekiq::Queue.new(@queue).find_job(@job.jid)
      worker = Class.new do
        attr_accessor :shutdown_called
        include SideJob::Worker
        def shutdown
          @shutdown_called = true
        end
      end.new
      worker.jid = sjob.jid
      chain = Sidekiq::Middleware::Chain.new
      chain.add SideJob::ServerMiddleware
      chain.invoke(worker, sjob.item, @queue) { raise 'should not be called' }
      expect(worker.shutdown_called).to be true
    end

    it 'logs but ignores exceptions thrown during shutdown' do
      SideJob.redis.hset @job.redis_key, 'status', 'terminating'
      sjob = Sidekiq::Queue.new(@queue).find_job(@job.jid)
      worker = Class.new do
        include SideJob::Worker
        def shutdown
          raise 'termination error'
        end
      end.new
      worker.jid = sjob.jid
      chain = Sidekiq::Middleware::Chain.new
      chain.add SideJob::ServerMiddleware
      expect { chain.invoke(worker, sjob.item, @queue) { raise 'should not be called' } }.to_not raise_error
      logs = @job.logs.select {|log| log['type'] == 'error'}
      expect(logs.size).to eq 1
      expect(logs[0]['error']).to eq 'termination error'
    end
  end
end
