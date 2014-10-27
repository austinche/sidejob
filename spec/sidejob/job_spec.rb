require 'spec_helper'

describe SideJob::Job do
  class TestWorkerDynamic
    include SideJob::Worker
    register(
        dynamic_inport: {default: 'def'},
        dynamic_outport: {default: 'def'},
    )
    def perform
    end
  end

  describe '#id=' do
    it 'reloads a job when changing id' do
      @job = SideJob.queue('testq', 'TestWorker')
      job2 = SideJob.queue('testq', 'TestWorkerEmpty')
      expect(@job.get(:class)).to eq 'TestWorker'
      @job.id = job2.id
      expect(@job.get(:class)).to eq 'TestWorkerEmpty'
    end

    it 'raises an error if job id does not exist' do
      @job = SideJob.queue('testq', 'TestWorker')
      expect { @job.id = 'missing' }.to raise_error
    end
  end

  describe '#==, #eql?' do
    it 'two jobs with the same id are eq' do
      expect(SideJob::Job.new('123')).to eq(SideJob::Job.new('123'))
      expect(SideJob::Job.new('123')).to eql(SideJob::Job.new('123'))
    end

    it 'two jobs with different id are not eq' do
      expect(SideJob::Job.new('123')).not_to eq(SideJob::Job.new('456'))
      expect(SideJob::Job.new('123')).not_to eql(SideJob::Job.new('456'))
    end
  end

  describe '#hash' do
    it 'uses hash of the job id and can be used as hash keys' do
      job = SideJob::Job.new('abc')
      expect(job.hash).to eq('abc'.hash)
      h = {}
      h[job] = 1
      job2 = SideJob::Job.new('abc')
      expect(job.hash).to eq(job2.hash)
      h[job2] = 3
      expect(h.keys.length).to be(1)
      expect(h[job]).to be(3)
    end
  end

  describe '#to_s' do
    it 'returns the redis key' do
      job = SideJob::Job.new('abc')
      expect(job.to_s).to eq 'job:abc'
    end
  end

  describe '#exists?' do
    it 'returns true if job exists' do
      @job = SideJob.queue('testq', 'TestWorker')
      expect(@job.exists?).to be true
    end
    it 'returns false if job does not exist' do
      expect(SideJob::Job.new('job').exists?).to be false
    end
  end

  describe '#log' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'adds a timestamp to log entries' do
      now = Time.now
      Time.stub(:now).and_return(now)
      @job.log('foo', {abc: 123})
      log = SideJob.redis.lpop "#{@job.redis_key}:log"
      expect(JSON.parse(log)).to eq({'type' => 'foo', 'abc' => 123, 'timestamp' => SideJob.timestamp})
    end

    it 'raises error if job no longer exists' do
      job2 = SideJob.find(@job.id)
      job2.status = 'terminated'
      job2.delete
      expect { @job.log('foo', {abc: 123}) }.to raise_error
    end
  end

  describe '#logs' do
    it 'returns all logs' do
      now = Time.now
      Time.stub(:now).and_return(now)
      job = SideJob.queue('testq', 'TestWorker')
      SideJob.redis.del "#{job.redis_key}:log"
      job.log('foo', {abc: 123})
      expect(job.logs).to eq([{'type' => 'foo', 'abc' => 123, 'timestamp' => SideJob.timestamp}])
      job.log('bar', {xyz: 456})
      expect(job.logs).to eq([{'type' => 'bar', 'xyz' => 456, 'timestamp' => SideJob.timestamp},
                              {'type' => 'foo', 'abc' => 123, 'timestamp' => SideJob.timestamp}])
    end

    it 'returns and clears all logs' do
      now = Time.now
      Time.stub(:now).and_return(now)
      job = SideJob.queue('testq', 'TestWorker')
      SideJob.redis.del "#{job.redis_key}:log"
      job.log('foo', {abc: 123})
      expect(job.logs(clear: true)).to eq([{'type' => 'foo', 'abc' => 123, 'timestamp' => SideJob.timestamp}])
      job.log('bar', {xyz: 456})
      expect(job.logs(clear: true)).to eq([{'type' => 'bar', 'xyz' => 456, 'timestamp' => SideJob.timestamp}])
    end
  end

  describe '#status' do
    it 'retrieves status' do
      @job = SideJob.queue('testq', 'TestWorker')
      expect(@job.status).to eq 'queued'
    end
  end

  describe '#status=' do
    it 'sets status' do
      @job = SideJob.queue('testq', 'TestWorker')
      @job.status = 'newstatus'
      expect(@job.status).to eq 'newstatus'
    end
  end

  describe '#terminate' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'sets the status to terminating' do
      @job.terminate
      expect(@job.status).to eq 'terminating'
    end

    it 'does nothing if status is terminated' do
      @job.status = 'terminated'
      @job.terminate
      expect(@job.status).to eq 'terminated'
    end

    it 'throws error and immediately sets status to terminated if job class is unregistered' do
      SideJob.redis.del 'workers:testq'
      expect { @job.terminate }.to raise_error
      expect(@job.status).to eq 'terminated'
    end

    it 'queues the job for termination run' do
      expect {
        @job.terminate
      }.to change {Sidekiq::Stats.new.enqueued}.by(1)
    end

    it 'by default does not terminate children' do
      child = SideJob.queue('testq', 'TestWorker', parent: @job, name: 'child')
      expect(child.status).to eq 'queued'
      @job.terminate
      expect(child.status).to eq 'queued'
    end

    it 'can recursively terminate' do
      5.times {|i| SideJob.queue('testq', 'TestWorker', parent: @job, name: "child#{i}") }
      @job.terminate(recursive: true)
      @job.children.each_value do |child|
        expect(child.status).to eq 'terminating'
      end
    end
  end

  describe '#run' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    %w{queued running suspended completed failed}.each do |status|
      it "queues the job if status is #{status}" do
        expect {
          @job.status = status
          @job.run
          expect(@job.status).to eq 'queued'
        }.to change {Sidekiq::Stats.new.enqueued}.by(1)
      end
    end

    %w{terminating terminated}.each do |status|
      it "does not queue the job if status is #{status}" do
        expect {
          @job.status = status
          @job.run
          expect(@job.status).to eq status
        }.to change {Sidekiq::Stats.new.enqueued}.by(0)
      end

      it "queues the job if status is #{status} and force=true" do
        expect {
          @job.status = status
          @job.run(force: true)
          expect(@job.status).to eq 'queued'
        }.to change {Sidekiq::Stats.new.enqueued}.by(1)
      end
    end

    it 'throws error and immediately sets status to terminated if job class is unregistered' do
      SideJob.redis.del "workers:#{@job.get(:queue)}"
      expect { @job.run }.to raise_error
      expect(@job.status).to eq 'terminated'
    end

    it 'can schedule a job to run at a specific time using a float' do
      time = Time.now.to_f + 10000
      expect { @job.run(at: time) }.to change {Sidekiq::Stats.new.scheduled_size}.by(1)
      expect(Sidekiq::ScheduledSet.new.find_job(@job.id).at).to eq(Time.at(time))
      expect(@job.status).to eq 'queued'
    end

    it 'can schedule a job to run at a specific time using a Time' do
      time = Time.now + 1000
      expect { @job.run(at: time) }.to change {Sidekiq::Stats.new.scheduled_size}.by(1)
      expect(Sidekiq::ScheduledSet.new.find_job(@job.id).at).to eq(Time.at(time.to_f))
      expect(@job.status).to eq 'queued'
    end

    it 'can schedule a job to run in a specific time' do
      now = Time.now
      Time.stub(:now).and_return(now)
      expect { @job.run(wait: 100) }.to change {Sidekiq::Stats.new.scheduled_size}.by(1)
      expect(Sidekiq::ScheduledSet.new.find_job(@job.id).at).to eq(Time.at(now.to_f + 100))
      expect(@job.status).to eq 'queued'
    end

    it 'raises error if job no longer exists' do
      job2 = SideJob.find(@job.id)
      job2.status = 'terminated'
      expect(job2.delete).to be true
      expect { @job.run }.to raise_error
    end
  end

  describe '#child' do
    it 'returns nil for missing child' do
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.child('child')).to be nil
    end

    it 'returns child by name' do
      job = SideJob.queue('testq', 'TestWorker')
      child = SideJob.queue('testq', 'TestWorker', parent: job, name: 'child')
      expect(job.child('child')).to eq child
    end
  end

  describe '#children, #parent' do
    it 'can get children and parent jobs' do
      parent = SideJob.queue('testq', 'TestWorker')
      child = SideJob.queue('testq', 'TestWorker', parent: parent, name: 'child')
      expect(parent.children).to eq('child' => child)
      expect(child.parent).to eq(parent)
    end
  end

  describe '#ancestors' do
    it 'returns empty array if no parent' do
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.ancestors).to eq([])
    end

    it 'returns entire job tree' do
      j1 = SideJob.queue('testq', 'TestWorker')
      j2 = SideJob.queue('testq', 'TestWorker', parent: j1, name: 'child')
      j3 = SideJob.queue('testq', 'TestWorker', parent: j2, name: 'child')
      j4 = SideJob.queue('testq', 'TestWorker', parent: j3, name: 'child')
      expect(j4.ancestors).to eq([j3, j2, j1])
    end
  end

  describe '#terminated?' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'returns false if job status is not terminated' do
      expect(@job.terminated?).to be false
    end

    it 'returns true if job status is terminated' do
      @job.status = 'terminated'
      expect(@job.terminated?).to be true
    end

    it 'returns false if child job is not terminated' do
      @job.status = 'terminated'
      SideJob.queue('testq', 'TestWorker', parent: @job, name: 'child')
      expect(@job.terminated?).to be false
    end

    it 'returns true if child job is terminated' do
      @job.status = 'terminated'
      child = SideJob.queue('testq', 'TestWorker', parent: @job, name: 'child')
      child.status = 'terminated'
      expect(@job.terminated?).to be true
    end
  end

  describe '#delete' do
    before do
      @job = SideJob.queue('testq', 'TestWorker', inports: {in1: {}}, outports: {out1: {}})
    end

    it 'does not delete non-terminated jobs' do
      expect(@job.delete).to be false
      expect(@job.exists?).to be true
    end

    it 'deletes terminated jobs' do
      @job.status = 'terminated'
      expect(@job.delete).to be true
      expect(@job.exists?).to be false
    end

    it 'recursively deletes jobs' do
      child = SideJob.queue('testq', 'TestWorker', parent: @job, name: 'child')
      expect(@job.status).to eq('queued')
      expect(child.status).to eq('queued')
      expect(SideJob.redis {|redis| redis.keys('job:*').length}).to be > 0
      @job.status = 'terminated'
      child.status = 'terminated'
      @job.delete
      expect(SideJob.redis {|redis| redis.keys('job:*').length}).to be(0)
      expect(@job.exists?).to be false
      expect(child.exists?).to be false
    end

    it 'deletes data on input and output ports' do
      @job.input(:in1).write 'data'
      @job.output(:out1).write 'data'
      expect(@job.input(:in1).size).to be 1
      expect(@job.output(:out1).size).to be 1
      @job.status = 'terminated'
      @job.delete
      expect(SideJob.redis {|redis| redis.keys('job:*').length}).to be(0)
    end
  end

  # Tests are identical for input and output port methods
  %i{in out}.each do |type|
    describe "##{type}put" do
      it "returns an #{type}put port" do
        spec = {}
        spec[:"#{type}ports"] = {port: {}}
        @job = SideJob.queue('testq', 'TestWorker', **spec)
        expect(@job.send("#{type}put", :port)).to eq(SideJob::Port.new(@job, type, :port))
      end

      it 'raises error on unknown port' do
        @job = SideJob.queue('testq', 'TestWorker')
        expect { @job.send("#{type}put", :unknown) }.to raise_error
      end

      it 'creates ports for workers configured with dynamic ports' do
        @job = SideJob.queue('testq', 'TestWorkerDynamic')
        expect {
          port = @job.send("#{type}put", :unknown)
          expect(port.size).to eq 0
          expect(port.read).to eq 'def'
        }.to change { @job.send("#{type}ports").size }.by(1)
      end
    end

    describe "##{type}ports" do
      it "returns all #{type}put ports" do
        @job = SideJob.queue('testq', 'TestWorker')
        expect(@job.send("#{type}ports")).to eq([SideJob::Port.new(@job, type, :static)])
      end
    end

    describe "##{type}ports=" do
      before do
        @job = SideJob.queue('testq', 'TestWorker')
      end

      it 'uses worker ports when nothing is given' do
        expect(@job.send("#{type}ports").size).to eq 1
      end

      it 'can specify additional ports with options' do
        @job.send("#{type}ports=", {myport: {mode: :memory, default: 'def'}})
        expect(@job.send("#{type}ports").size).to eq 2
        expect(@job.send("#{type}ports").map(&:name)).to include(:myport)
        expect(@job.send("#{type}put", :myport).mode).to eq :memory
        expect(@job.send("#{type}put", :myport).default).to eq 'def'
      end

      it 'can change existing port mode while keeping data intact' do
        @job.send("#{type}put", :static).write 'data'
        @job.send("#{type}ports=", {static: {mode: :memory, default: 'def'}})
        expect(@job.send("#{type}ports").size).to eq 1
        expect(@job.send("#{type}put", :static).mode).to eq :memory
        expect(@job.send("#{type}put", :static).default).to eq 'def'
        expect(@job.send("#{type}put", :static).read).to eq 'data'
      end

      it 'deletes no longer used ports' do
        @job.send("#{type}ports=", {myport: {}})
        @job.send("#{type}put", :myport).write 123
        @job.send("#{type}ports=", {})
        expect(@job.send("#{type}ports").map(&:name)).not_to include(:myport)
        expect { @job.send("#{type}put", :myport) }.to raise_error
      end
    end
  end

  describe '#get' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
      SideJob.redis.hset 'job', @job.id, { queue: 'testq', class: 'TestWorker', field1: 'value1', field2: [1,2], field3: 123 }.to_json
      @job.reload
    end

    it 'returns a value from job state using symbol key' do
      expect(@job.get(:field3)).to eq 123
    end

    it 'returns a value from job state using string key' do
      expect(@job.get('field1')).to eq 'value1'
    end

    it 'returns nil for missing value' do
      expect(@job.get(:missing)).to be nil
    end

    it 'can retrieve complex objects in job state' do
      expect(@job.get(:field2)).to eq [1, 2]
    end

    it 'caches the state' do
      expect(@job.get(:field3)).to eq 123
      SideJob.redis.hmset @job.redis_key, :field3, '789'
      expect(@job.get(:field3)).to eq 123
    end

    it 'raises error if job no longer exists and state is not cached' do
      @job.reload
      job2 = SideJob.find(@job.id)
      job2.status = 'terminated'
      job2.delete
      expect { @job.get(:key) }.to raise_error
    end

    it 'does not raise error if job no longer exists but state is cached' do
      @job.get(:foo)
      job2 = SideJob.find(@job.id)
      job2.status = 'terminated'
      job2.delete
      expect { @job.get(:key) }.not_to raise_error
    end
  end

  describe '#reload' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'clears the job state cache' do
      expect(@job.get(:field1)).to be nil
      SideJob.redis.hset 'job', @job.id, {queue: 'testq', class: 'TestWorker', field1: 789}.to_json
      @job.reload
      expect(@job.get(:field1)).to eq 789
    end
  end

  describe '#config' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'returns and caches worker configuration' do
      expect(SideJob::Worker).to receive(:config).with('testq', 'TestWorker').once.and_call_original
      @job.reload
      @job.config
      @job.config # test cached
    end
  end
end
