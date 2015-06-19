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
      expect(job.id).to be 1
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.id).to be 2
    end

    it 'stores created at timestamp' do
      now = Time.now
      allow(Time).to receive(:now) { now }
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.info[:created_at]).to eq(SideJob.timestamp)
    end

    it 'can specify job args' do
      job = SideJob.queue('testq', 'TestWorker', args: [1,2])
      expect(job.status).to eq 'queued'
      expect(job.info[:args]).to eq [1,2]
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

    it 'can add a port via inports configuration' do
      job = SideJob.queue('testq', 'TestWorker', inports: {myport: {default: [1,2]}})
      expect(job.status).to eq 'queued'
      expect(job.input(:myport).read).to eq [1, 2]
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
      expect(job.info[:created_by]).to eq 'test:sidejob'
    end

    it 'defaults to empty by string' do
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.get(:created_by)).to be nil
    end
  end

  describe '.find' do
    it 'returns a job object by id' do
      job = SideJob.queue('testq', 'TestWorker')
      expect(SideJob.find(job.id)).to eq(job)
    end

    it 'returns a job object by alias' do
      job = SideJob.queue('testq', 'TestWorker')
      job.add_alias 'myjob'
      expect(SideJob.find('myjob')).to eq(job)
    end

    it 'returns nil if the job does not exist' do
      expect(SideJob.find('job')).to be_nil
    end
  end

  describe '.timestamp' do
    it 'returns subseconds' do
      expect(SideJob.timestamp).to match /T\d\d:\d\d:\d\d\./
    end
  end

  describe '.log' do
    it 'adds a timestamp to log entries' do
      now = Time.now
      allow(Time).to receive(:now) { now }
      expect(SideJob).to receive(:publish).with('/sidejob/log', {abc: 123, timestamp: SideJob.timestamp}, {disable_log: true})
      SideJob.log({abc: 123})
    end

    it 'does not generate an infinite publish loop for port subscriptions on /sidejob/log' do
      job = SideJob.queue('testq', 'TestWorker', inports: {port1: {channels: ['/sidejob/log', '/']}})
      SideJob.log({test: 1})
    end
  end

  describe '.context' do
    before do
      now = Time.now
      allow(Time).to receive(:now) { now }
    end

    it 'adds data to logs within the group' do
      expect(SideJob).to receive(:publish).with('/sidejob/log', {data1: 1, data2: 2, abc: 123, timestamp: SideJob.timestamp}, {disable_log: true})
      SideJob.context(data1: 1, data2: 2) do
        SideJob.log({abc: 123})
      end
    end

    it 'does not add data to logs outside of the group' do
      expect(SideJob).to receive(:publish).with('/sidejob/log', {abc: 123, timestamp: SideJob.timestamp}, {disable_log: true})
      SideJob.context(data1: 1, data2: 2) {}
      SideJob.log({abc: 123})
    end

    it 'can be nested' do
      expect(SideJob).to receive(:publish).with('/sidejob/log', {data1: 1, timestamp: SideJob.timestamp, x: 1}, {disable_log: true})
      expect(SideJob).to receive(:publish).with('/sidejob/log', {data1: 1, data2: 2, timestamp: SideJob.timestamp, x: 2}, {disable_log: true})
      expect(SideJob).to receive(:publish).with('/sidejob/log', {data1: 1, timestamp: SideJob.timestamp, x: 3}, {disable_log: true})
      expect(SideJob).to receive(:publish).with('/sidejob/log', {timestamp: SideJob.timestamp, x: 4}, {disable_log: true})
      SideJob.context(data1: 1) do
        SideJob.log({x: 1})
        SideJob.context(data2: 2) do
          SideJob.log({x: 2})
        end
        SideJob.log({x: 3})
      end
      SideJob.log({x: 4})
    end
  end

  describe '.publish' do
    it 'publishes message to channel ignoring hierarchy using redis pubsub' do
      Timeout::timeout(3) do
        subscribed = false
        t = Thread.new do
          redis = SideJob.redis.dup
          redis.psubscribe('*') do |on|
            on.psubscribe do |pattern, total|
              subscribed = true
            end

            on.pmessage do |pattern, channel, message|
              expect(JSON.parse(message)).to eq [1,2]
              expect(channel).to eq '/namespace/mychannel'
              redis.punsubscribe
            end
          end
        end

        Thread.pass until subscribed
        SideJob.publish '/namespace/mychannel', [1,2]
        t.join
      end
    end

    it 'writes to subscribed jobs' do
      job = SideJob.queue('testq', 'TestWorker', inports: {myport: {channels: ['/namespace/mychannel']}, yourport: {channels: ['/namespace']}})
      SideJob.publish('/namespace/mychannel', [1,2])
      expect(job.input(:myport).entries).to eq [[1,2]]
      expect(job.input(:yourport).entries).to eq [[1,2]]
    end

    it 'includes original channel in context' do
      job = SideJob.queue('testq', 'TestWorker', inports: {myport: {channels: ['/namespace']}})
      SideJob.publish('/namespace/mychannel', [1,2])
      data = job.input(:myport).read
      expect(data).to eq [1,2]
      expect(data.sidejob_context).to eq({'channel' => '/namespace/mychannel'})
    end

    it 'removes jobs that are no longer subscribed' do
      job = SideJob.queue('testq', 'TestWorker', inports: {myport: {channels: ['/namespace/mychannel']}})
      job.input(:myport).channels = []
      expect(SideJob.redis.smembers('channel:/namespace/mychannel')).to eq [job.id.to_s]
      SideJob.publish('/namespace/mychannel', [1,2])
      expect(SideJob.redis.smembers('channel:/namespace/mychannel')).to eq []
      expect(job.input(:myport).size).to eq 0
    end
  end
end
