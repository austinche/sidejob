require 'spec_helper'

describe SideJob do
  describe '.redis' do
    it 'returns Redis instance via Sidekiq' do
      r1 = SideJob.redis {|redis| redis}
      r2 = Sidekiq.redis {|redis| redis}
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

    it 'can specify job args' do
      expect {
        job = SideJob.queue('testq', 'TestWorker', {args: [1, 2]})
        expect(job.status).to eq(:queued)
      }.to change(TestWorker.jobs, :size).by(1)
      expect(TestWorker.jobs.last['args']).to eq([1,2])
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
