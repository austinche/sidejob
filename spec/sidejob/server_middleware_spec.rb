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

  it 'sets status to :running on start' do
    @chain.invoke(@worker, {}, @queue) do
      @status = @job.status
    end
    expect(@status).to be(:running)
  end

  it 'sets status to completed on completion' do
    expect(@job.status).to be(:queued)
    @chain.invoke(@worker, {}, @queue) {}
    expect(@job.status).to be(:completed)
  end

  it 'sets status to failed on exception and logs error' do
    now = Time.now
    Time.stub(:now).and_return(now)
    expect { @chain.invoke(@worker, {}, @queue) do
      raise 'oops'
    end }.to raise_error
    expect(@job.status).to be(:failed)
    
    log = nil
    while l = @job.log_pop do
      log = l if l['type'] == 'error'
    end
    expect(log['error']).to eq('oops')
    expect(log['backtrace'].class).to eq(Array)
  end

  it 'restarts the worker if it is restarted while running' do
    expect { 
      @chain.invoke(@worker, {}, @queue) do
        @job.restart
      end
    }.to change(TestWorker.jobs, :size).by(1)
    expect(@job.status).to be(:queued)
  end
end
