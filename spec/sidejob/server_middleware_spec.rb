require 'spec_helper'

describe SideJob::ServerMiddleware do
  before do
    @queue = 'testq'
    @job = SideJob.queue(@queue, 'TestWorker')
  end

  def process(job)
    sjob = Sidekiq::Queue.new(@queue).find_job(job.jid)
    worker = TestWorker.new
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

  describe 'handles a normal run' do
    it 'sets the current job to the worker and resets at end' do
      process(@job) { @current = Thread.current[:SideJob] }
      expect(@current).to eq(@job)
      expect(Thread.current[:SideJob]).to be nil
    end

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
      process(@job) { @ran_at = SideJob.redis.hget(@job.redis_key, 'ran_at').to_f }
      expect(@ran_at).to eq now.to_f
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
      SideJob.redis.hset @job.redis_key, 'ran_at', (Time.now+100).to_f
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
    it 'does not run if called too many times in a second' do
      now = Time.now
      Time.stub(:now).and_return(now)
      key = "#{@job.redis_key}:rate:#{Time.now.to_i}"
      SideJob.redis.set key, SideJob::ServerMiddleware::MAX_CALLS_PER_SECOND
      @run = false
      process(@job) { @run = true }
      expect(@run).to be false
      expect(@job.status).to eq 'terminated'
    end

    it 'does run if not called too many times in a second' do
      now = Time.now
      Time.stub(:now).and_return(now)
      key = "#{@job.redis_key}:rate:#{Time.now.to_i}"
      SideJob.redis.set key, SideJob::ServerMiddleware::MAX_CALLS_PER_SECOND-1
      @run = false
      process(@job) { @run = true }
      expect(@run).to be true
      expect(@job.status).to eq 'completed'
    end

    it 'does not run if job is too deep' do
      (SideJob::ServerMiddleware::MAX_JOB_DEPTH+1).times do |i|
        @job = SideJob.queue(@queue, 'TestWorker', {parent: @job})
      end
      @run = false
      process(@job) { @run = true }
      expect(@run).to be false
      expect(@job.status).to eq 'terminated'
    end

    it 'does run if job is not too deep' do
      SideJob::ServerMiddleware::MAX_JOB_DEPTH.times do |i|
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
      process(@job) {}
      expect(@job.status).to eq 'terminated'
    end

    it 'logs terminated status change' do
      SideJob.redis.hset @job.redis_key, 'status', 'terminating'
      process(@job) {}
      log = @job.logs.detect {|log| log['type'] == 'status'}
      expect(log['status']).to eq 'terminated'
    end

    it 'runs parent' do
      child = SideJob.queue(@queue, 'TestWorker', {parent: @job})
      SideJob.redis.hset child.redis_key, 'status', 'terminating'
      process(child) {}
      expect(child.status).to eq 'terminated'
      expect(@job.status).to eq 'queued'
    end

    it 'calls worker shutdown method' do
      sjob = Sidekiq::Queue.new(@queue).find_job(@job.jid)
      worker = Class.new do
        attr_accessor :shutdown
        include SideJob::Worker
        def shutdown
          @shutdown = true
        end
      end.new
      worker.jid = sjob.jid
      chain = Sidekiq::Middleware::Chain.new
      chain.add SideJob::ServerMiddleware
      chain.invoke(worker, sjob.item, @queue) { }
      expect(worker.shutdown).to be true
    end
  end
end
