require 'spec_helper'

describe SideJob::Worker do
  before do
    @job = SideJob.queue('testq', 'TestWorker', [1])
    @worker = TestWorker.new
    @worker.jid = @job
  end

  it 'includes SideJob::JobMethods' do
    expect(TestWorker.included_modules).to include(SideJob::JobMethods)
  end

  it '#at stores progress' do
    @worker.at(23, 46)
    expect(@worker.mget(:num, :total)).to eq({:num => '23', :total => '46'})
  end

  it '#suspend raises exception' do
    expect { @worker.suspend }.to raise_error(SideJob::Worker::Suspended)
  end
end
