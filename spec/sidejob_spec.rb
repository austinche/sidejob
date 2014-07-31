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
        job = SideJob.queue('testq', 'TestWorker')
        expect(job.status).to eq(:queued)
      }.to change(TestWorker.jobs, :size).by(1)
      expect(TestWorker.jobs.last['queue']).to eq('testq')
      expect(TestWorker.jobs.last['class']).to eq('TestWorker')
    end

    it 'can specify a parent job' do
      parent = SideJob.queue('testq', 'TestWorker')
      expect(parent.children).to eq([])
      child = parent.queue('testq', 'TestWorker')
      expect(parent.children).to eq([child])
      expect(child.parent).to eq(parent)
    end
  end

  describe '.find' do
    it 'returns a job object by id' do
      job = SideJob.queue('testq', 'TestWorker')
      expect(SideJob.find(job.jid)).to eq(job)
    end

    it 'returns nil if the job does not exist' do
      expect(SideJob.find('job')).to be_nil
    end
  end
end
