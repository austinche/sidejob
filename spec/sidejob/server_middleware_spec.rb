require 'spec_helper'

describe SideJob::ServerMiddleware do
  before do
    @queue = 'testq'
    @job = SideJob.queue(@queue, 'TestWorker')
    @worker = TestWorker.new
    @worker.jid = @job.jid
    @chain = Sidekiq::Middleware::Chain.new
    @chain.add SideJob::ServerMiddleware
  end

  it 'sets the current job to the worker' do
    @chain.invoke(@worker, {}, @queue) do
      @current = Thread.current[:SideJob]
    end
    expect(@current).to eq(@worker)
    expect(@worker.status).to be(:completed)
  end

  it 'sets status to :running on start' do
    @chain.invoke(@worker, {}, @queue) do
      @status = @job.status
    end
    expect(@status).to be(:running)
  end

  it 'does not run if status is :stopped' do
    @worker.status = :stopped
    @run = false
    @chain.invoke(@worker, {}, @queue) do
      @run = true
    end
    expect(@run).to be false
    expect(@worker.status).to be(:stopped)
  end

  it 'does not run if called too many times in a second' do
    now = Time.now
    Time.stub(:now).and_return(now)
    key = "#{@worker.redis_key}:rate:#{Time.now.to_i}"
    SideJob.redis.set key, SideJob::ServerMiddleware::MAX_CALLS_PER_SECOND + 1
    @run = false
    @chain.invoke(@worker, {}, @queue) do
      @run = true
    end
    expect(@run).to be false
    expect(@worker.status).to be(:stopped)
  end

  it 'does run if not called too many times in a second' do
    now = Time.now
    Time.stub(:now).and_return(now)
    key = "#{@worker.redis_key}:rate:#{Time.now.to_i}"
    SideJob.redis.set key, SideJob::ServerMiddleware::MAX_CALLS_PER_SECOND
    @run = false
    @chain.invoke(@worker, {}, @queue) do
      @run = true
    end
    expect(@run).to be true
    expect(@worker.status).to be(:completed)
  end

  it 'does not run if job is too deep' do
    (SideJob::ServerMiddleware::MAX_JOB_DEPTH+1).times do |i|
      @job = SideJob.queue(@queue, 'TestWorker', {parent: @job})
    end
    @worker.jid = @job.jid
    @run = false
    @chain.invoke(@worker, {}, @queue) do
      @run = true
    end
    expect(@run).to be false
    expect(@worker.status).to be(:stopped)
  end

  it 'does run if job is not too deep' do
    SideJob::ServerMiddleware::MAX_JOB_DEPTH.times do |i|
      @job = SideJob.queue(@queue, 'TestWorker', {parent: @job})
    end
    @worker.jid = @job.jid
    @run = false
    @chain.invoke(@worker, {}, @queue) do
      @run = true
    end
    expect(@run).to be true
    expect(@worker.status).to be(:completed)
  end

  it 'sets status to completed on completion' do
    expect(@job.status).to be(:queued)
    @chain.invoke(@worker, {}, @queue) {}
    expect(@job.status).to be(:completed)
  end

  it 'sets status to failed on exception and logs error' do
    now = Time.now
    Time.stub(:now).and_return(now)
    @chain.invoke(@worker, {}, @queue) do
      raise 'oops'
    end
    expect(@job.status).to be(:failed)
    
    log = SideJob.redis.lrange("#{@job.redis_key}:log", 0, -1).
        map {|log| JSON.parse(log) }.select {|log| log['type'] == 'error'}
    expect(log.size).to eq(1)
    expect(log[0]['error']).to eq('oops')
    # check that we trim down backtrace to remove sidekiq lines
    expect(log[0]['backtrace']).to_not match(/sidekiq/)
  end

  it 'restarts the worker if it is restarted while running' do
    expect { 
      @chain.invoke(@worker, {}, @queue) do
        @job.restart
      end
    }.to change {Sidekiq::Stats.new.enqueued}.by(1)
    expect(@job.status).to be(:queued)
  end
end
