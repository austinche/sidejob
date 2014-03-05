require 'spec_helper'

describe SideJob::ServerMiddleware do
  before do
    @queue = 'testq'
    @job = SideJob.queue(@queue, 'TestWorker', [1])
    @worker = TestWorker.new
    @worker.jid = @job.jid
    @chain = Sidekiq::Middleware::Chain.new
    @chain.add SideJob::ServerMiddleware
  end

  it 'sets status to working on start' do
    @chain.invoke(@worker, {}, @queue) do
      @status = @job.status
    end
    expect(@status).to be(:working)
  end

  it 'sets status to completed on completion' do
    expect(@job.status).to be(:queued)
    @chain.invoke(@worker, {}, @queue) {}
    expect(@job.status).to be(:completed)
  end

  it 'sets status to suspended on suspend' do
    @chain.invoke(@worker, {}, @queue) do
      @worker.suspend
    end
    expect(@job.status).to be(:suspended)
  end

  it 'sets status to failed on exception' do
    @chain.invoke(@worker, {}, @queue) do
      raise 'oops'
    end
    expect(@job.status).to be(:failed)
    expect(@job.get(:error)).to eq('oops')
  end

  it 'restarts the worker if status is restarting' do
    expect { 
      @chain.invoke(@worker, {}, @queue) do
        @job.status = :restarting
      end
    }.to change(TestWorker.jobs, :size).by(1)
    expect(@job.status).to be(:queued)
  end
end
