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

    it 'generates an incrementing job id from 1' do
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.jid).to eq('1')
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.jid).to eq('2')
    end

    it 'can specify job parent' do
      expect {
        parent = SideJob.queue('testq', 'TestWorker')
        job = SideJob.queue('testq', 'TestWorker', {parent: parent})
        expect(job.status).to eq(:queued)
        expect(job.parent).to eq(parent)
      }.to change(TestWorker.jobs, :size).by(2)
    end

    it 'can specify job args' do
      expect {
        job = SideJob.queue('testq', 'TestWorker', {args: [1, 2]})
        expect(job.status).to eq(:queued)
      }.to change(TestWorker.jobs, :size).by(1)
      expect(TestWorker.jobs.last['args']).to eq([1,2])
    end

    it 'can specify a job time' do
      at = Time.now.to_f + 1000
      expect {
        job = SideJob.queue('testq', 'TestWorker', {at: at})
        expect(job.status).to eq(:scheduled)
      }.to change(TestWorker.jobs, :size).by(1)
      expect(TestWorker.jobs.last['at']).to eq(at)
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
