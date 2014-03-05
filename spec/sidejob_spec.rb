require 'spec_helper'

describe SideJob do
  describe '.redis' do
    it 'returns Redis instance via Sidekiq' do
      r1 = SideJob.redis {|conn| conn}
      r2 = Sidekiq.redis {|conn| conn}
      expect(r1).to be(r2)
    end
  end

  describe '.queue' do
    it 'queues a sidekiq job' do
      expect {
        job = SideJob.queue('testq', 'TestWorker', [1, 2, 3])
        expect(job.status).to eq(:queued)
      }.to change(TestWorker.jobs, :size).by(1)
      expect(TestWorker.jobs.last['args']).to eq([1, 2, 3])
    end

    it 'can specify a parent job' do
      parent = SideJob.queue('testq', 'TestWorker', [1])
      expect(parent.children).to eq([])
      child = parent.queue('testq', 'TestWorker', [2])
      expect(parent.children).to eq([child])
      expect(child.parent).to eq(parent)
    end
  end
end
