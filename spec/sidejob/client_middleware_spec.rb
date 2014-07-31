require 'spec_helper'

describe SideJob::ClientMiddleware do
  it 'job status is set to queued initially' do
    job = SideJob.queue('testq', 'TestWorker')
    expect(job.status).to eq(:queued)
  end
end
