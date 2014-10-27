require 'spec_helper'

describe SideJob do
  describe '.redis' do
    it 'returns Redis instance via Sidekiq' do
      r1 = SideJob.redis {|redis| redis}
      r2 = Sidekiq.redis {|redis| redis}
      expect(r1).to be(r2)
    end

    it 'returns Redis without block' do
      expect(SideJob.redis {|redis| redis}).to be(SideJob.redis)
    end
  end

  describe '.redis=' do
    it 'sets redis url' do
      original = SideJob.redis.client.options[:url]
      SideJob.redis = {url: 'redis://myredis:1234/10'}
      expect(SideJob.redis.client.options[:url]).to eq('redis://myredis:1234/10')
      expect(SideJob.redis.client.options[:host]).to eq('myredis')
      expect(SideJob.redis.client.options[:port]).to eq(1234)
      expect(SideJob.redis.client.options[:db]).to eq(10)
      SideJob.redis = {url: original}
    end
  end

  describe '.queue' do
    it 'raises an error if no worker registered for specified queue/class' do
      expect { SideJob.queue('unknownq', 'TestWorker') }.to raise_error
      expect { SideJob.queue('testq', 'UnknownWorker') }.to raise_error
    end

    it 'queues a sidekiq job' do
      expect {
        job = SideJob.queue('testq', 'TestWorker')
        expect(job.exists?).to be true
        expect(job.status).to eq 'queued'
        job = Sidekiq::Queue.new('testq').find_job(job.id)
        expect(job.queue).to eq('testq')
        expect(job.klass).to eq('TestWorker')
        expect(job.args).to eq([])
      }.to change {Sidekiq::Stats.new.enqueued}.by(1)
    end

    it 'generates an incrementing job id from 1' do
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.id).to eq('1')
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.id).to eq('2')
    end

    it 'stores created at timestamp' do
      now = Time.now
      allow(Time).to receive(:now) { now }
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.get(:created_at)).to eq(SideJob.timestamp)
    end

    it 'can specify job args' do
      job = SideJob.queue('testq', 'TestWorker', args: [1,2])
      expect(job.status).to eq 'queued'
      expect(job.get(:args)).to eq [1,2]
      expect_any_instance_of(TestWorker).to receive(:perform).with(1, 2)
      SideJob::Worker.drain_queue
    end

    it 'can specify job parent' do
      expect {
        parent = SideJob.queue('testq', 'TestWorker')
        job = SideJob.queue('testq', 'TestWorker', parent: parent, name: 'child1')
        expect(job.status).to eq 'queued'
        expect(job.parent).to eq(parent)
        expect(parent.child('child1')).to eq job
        expect(SideJob.redis.lrange("#{job.redis_key}:ancestors", 0, -1)).to eq([parent.id])
      }.to change {Sidekiq::Stats.new.enqueued}.by(2)
    end

    it 'raises an error if name: option not specified with parent' do
      parent = SideJob.queue('testq', 'TestWorker')
      expect { SideJob.queue('testq', 'TestWorker', parent: parent) }.to raise_error
    end

    it 'raises an error if name: name is not unique' do
      parent = SideJob.queue('testq', 'TestWorker')
      SideJob.queue('testq', 'TestWorker', parent: parent, name: 'child')
      expect { SideJob.queue('testq', 'TestWorker', parent: parent, name: 'child') }.to raise_error
    end

    it 'sets ancestor tree correctly' do
      j1 = SideJob.queue('testq', 'TestWorker')
      j2 = SideJob.queue('testq', 'TestWorker', parent: j1, name: 'child1')
      j3 = SideJob.queue('testq', 'TestWorker', parent: j2, name: 'child1')
      expect(SideJob.redis.lrange("#{j3.redis_key}:ancestors", 0, -1)).to eq([j2.id, j1.id])
    end

    it 'can add a port via inports configuration' do
      job = SideJob.queue('testq', 'TestWorker', inports: {myport: {default: [1,2]}})
      expect(job.status).to eq 'queued'
      expect(job.input(:myport).read).to eq [1, 2]
    end

    it 'can set the port mode via inports configuration' do
      job = SideJob.queue('testq', 'TestWorker', inports: {queue: {mode: 'queue'}})
      expect(job.status).to eq 'queued'
      expect(job.input(:queue).mode).to be :queue
    end

    it 'can add a port via outports configuration' do
      job = SideJob.queue('testq', 'TestWorker', outports: {myport: {}})
      expect(job.status).to eq 'queued'
      expect(job.outports.map(&:name).include?(:myport)).to be true
    end

    it 'can specify a job time' do
      at = Time.now.to_f + 1000
      expect {
        job = SideJob.queue('testq', 'TestWorker', at: at)
        expect(job.status).to eq 'queued'
        expect(Sidekiq::ScheduledSet.new.find_job(job.id).at).to eq(Time.at(at))
      }.to change {Sidekiq::Stats.new.scheduled_size}.by(1)
    end

    it 'can specify a by string' do
      job = SideJob.queue('testq', 'TestWorker', by: 'test:sidejob')
      expect(job.by).to eq 'test:sidejob'
    end

    it 'defaults to empty by string' do
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.by).to be nil
    end
  end

  describe '.find' do
    it 'returns a job object by id' do
      job = SideJob.queue('testq', 'TestWorker')
      expect(SideJob.find(job.id)).to eq(job)
    end

    it 'returns nil if the job does not exist' do
      expect(SideJob.find('job')).to be_nil
    end

    it 'can specify a by string' do
      job = SideJob.queue('testq', 'TestWorker', by: 'test:orig')
      job2 = SideJob.find(job.id, by: 'test:by')
      expect(job2.by).to eq 'test:by'
    end
  end

  describe '.timestamp' do
    it 'returns subseconds' do
      expect(SideJob.timestamp).to match /T\d\d:\d\d:\d\d\./
    end
  end
end
