require 'spec_helper'

describe SideJob::Worker do
  before do
    @job = SideJob.queue('testq', 'TestWorker')
    @job.status = :running
    @worker = TestWorker.new
    @worker.jid = @job
  end

  it 'includes SideJob::JobMethods' do
    expect(TestWorker.included_modules).to include(SideJob::JobMethods)
  end

  it '#suspend sets status to suspended' do
    @worker.suspend
    expect(@worker.status).to eq(:suspended)
  end

  it 'provides a worker registry' do
    expect(SideJob::Worker.all).to eq []
    spec = {foo: 'bar'}
    @worker.class.register(spec)
    expect(SideJob::Worker.all).to eq [spec]
  end
end
