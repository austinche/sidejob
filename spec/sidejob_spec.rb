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
        job = Sidekiq::Queue.new('testq').find_job(job.jid)
        expect(job.queue).to eq('testq')
        expect(job.klass).to eq('TestWorker')
        expect(job.args).to eq([])
      }.to change {Sidekiq::Stats.new.enqueued}.by(1)
    end

    it 'generates an incrementing job id from 1' do
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.jid).to eq('1')
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.jid).to eq('2')
    end

    it 'stores jid in jobs set' do
      job = SideJob.queue('testq', 'TestWorker')
      expect(SideJob.redis {|redis| redis.sismember('jobs', job.jid)}).to be true
    end

    it 'can specify job parent' do
      expect {
        parent = SideJob.queue('testq', 'TestWorker')
        job = SideJob.queue('testq', 'TestWorker', {parent: parent})
        expect(job.status).to eq(:queued)
        expect(job.parent).to eq(parent)
      }.to change {Sidekiq::Stats.new.enqueued}.by(2)
    end

    it 'can specify job args' do
      expect {
        job = SideJob.queue('testq', 'TestWorker', {args: [1, 2]})
        expect(job.status).to eq(:queued)
        job = Sidekiq::Queue.new('testq').find_job(job.jid)
        expect(job.args).to eq([1, 2])
      }.to change {Sidekiq::Stats.new.enqueued}.by(1)
    end

    it 'can specify a job time' do
      at = Time.now.to_f + 1000
      expect {
        job = SideJob.queue('testq', 'TestWorker', {at: at})
        expect(job.status).to eq(:scheduled)
        expect(Sidekiq::ScheduledSet.new.find_job(job.jid).at).to eq(Time.at(at))
      }.to change {Sidekiq::Stats.new.scheduled_size}.by(1)
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
