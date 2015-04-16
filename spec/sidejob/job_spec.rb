require 'spec_helper'

describe SideJob::Job do
  describe '#initialize' do
    it 'raises error if job does not exist' do
      expect { SideJob::Job.new('123') }.to raise_error
    end
  end

  describe '#==, #eql?' do
    it 'two jobs with the same id are eq' do
      @job = SideJob.queue('testq', 'TestWorker')
      expect(SideJob::Job.new(@job.id)).to eq(@job)
      expect(SideJob::Job.new(@job.id)).to eql(@job)
    end

    it 'two jobs with different id are not eq' do
      @job = SideJob.queue('testq', 'TestWorker')
      @job2 = SideJob.queue('testq', 'TestWorker')
      expect(@job).not_to eq(@job2)
      expect(@job).not_to eql(@job2)
    end
  end

  describe '#hash' do
    it 'uses hash of the job id and can be used as hash keys' do
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.hash).to eq(job.id.hash)
      h = {}
      h[job] = 1
      job2 = SideJob::Job.new(job.id)
      expect(job.hash).to eq(job2.hash)
      h[job2] = 3
      expect(h.keys.length).to be(1)
      expect(h[job]).to be(3)
    end
  end

  describe '#to_s' do
    it 'returns the redis key' do
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.to_s).to eq "job:#{job.id}"
    end
  end

  describe '#exists?' do
    it 'returns true if job exists' do
      @job = SideJob.queue('testq', 'TestWorker')
      expect(@job.exists?).to be true
    end

    it 'returns false if job no longer exists' do
      @job = SideJob.queue('testq', 'TestWorker')
      @job.status = 'terminated'
      @job.delete
      expect(@job.exists?).to be false
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
          expect(@job.run).to eq @job
          expect(@job.status).to eq 'queued'
        }.to change {Sidekiq::Stats.new.enqueued}.by(1)
      end
    end

    %w{terminating terminated}.each do |status|
      it "does not queue the job if status is #{status}" do
        expect {
          @job.status = status
          expect(@job.run).to be nil
          expect(@job.status).to eq status
        }.to change {Sidekiq::Stats.new.enqueued}.by(0)
      end

      it "queues the job if status is #{status} and force=true" do
        expect {
          @job.status = status
          expect(@job.run(force: true)).to eq @job
          expect(@job.status).to eq 'queued'
        }.to change {Sidekiq::Stats.new.enqueued}.by(1)
      end
    end

    it 'does nothing if no parent job and parent=true' do
      @job.status = 'completed'
      expect {
        expect(@job.run(parent: true)).to be nil
        expect(@job.status).to eq 'completed'
      }.to change {Sidekiq::Stats.new.enqueued}.by(0)
    end

    it 'runs parent job if parent=true' do
      parent = SideJob.queue('testq', 'TestWorker')
      parent.adopt(@job, 'child')
      @job.status = 'completed'
      parent.status = 'completed'
      expect {
        expect(@job.run(parent: true)).to eq parent
        expect(@job.status).to eq 'completed'
        expect(parent.status).to eq 'queued'
      }.to change {Sidekiq::Stats.new.enqueued}.by(1)
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
      allow(Time).to receive(:now) { now }
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

  describe '#queue' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'can queue child jobs' do
      expect(SideJob).to receive(:queue).with('testq', 'TestWorker', args: [1,2], inports: {'myport' => {'mode' => 'memory'}}, parent: @job, name: 'child', by: "job:#{@job.id}").and_call_original
      expect {
        child = @job.queue('testq', 'TestWorker', args: [1,2], inports: {'myport' => {'mode' => 'memory'}}, name: 'child')
        expect(child.parent).to eq(@job)
        expect(@job.children).to eq('child' => child)
      }.to change {Sidekiq::Stats.new.enqueued}.by(1)
    end

    it 'queues with by string set to self' do
      child = @job.queue('testq', 'TestWorker', name: 'child')
      expect(child.get(:created_by)).to eq "job:#{@job.id}"
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

  describe '#disown' do
    it 'raises error if child cannot be found' do
      parent = SideJob.queue('testq', 'TestWorker')
      expect { parent.disown('child') }.to raise_error
    end

    it 'disassociates a child job from the parent' do
      parent = SideJob.queue('testq', 'TestWorker')
      child = SideJob.queue('testq', 'TestWorker', parent: parent, name: 'child')
      expect(child.parent).to eq(parent)
      expect(parent.child('child')).to eq child
      parent.disown('child')
      expect(child.parent).to be nil
      expect(parent.child('child')).to be nil
    end
  end

  describe '#adopt' do
    it 'can adopt an orphan job' do
      job = SideJob.queue('testq', 'TestWorker')
      child = SideJob.queue('testq', 'TestWorker')
      expect(child.parent).to be nil
      job.adopt(child, 'child')
      expect(child.parent).to eq(job)
    end

    it 'raises error when adopting self' do
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.parent).to be nil
      expect { job.adopt(job, 'self') }.to raise_error
    end

    it 'raises error if job already has a parent' do
      job = SideJob.queue('testq', 'TestWorker')
      job2 = SideJob.queue('testq', 'TestWorker')
      child = SideJob.queue('testq', 'TestWorker')
      job.adopt(child, 'child')
      expect { job2.adopt(child, 'mine') }.to raise_error
    end

    it 'raises error if no name is given' do
      job = SideJob.queue('testq', 'TestWorker')
      child = SideJob.queue('testq', 'TestWorker')
      expect { job.adopt(child, nil) }.to raise_error
    end

    it 'raises error if name is not unique' do
      job = SideJob.queue('testq', 'TestWorker')
      child = SideJob.queue('testq', 'TestWorker')
      child2 = SideJob.queue('testq', 'TestWorker')
      job.adopt(child, 'child')
      expect { job.adopt(child2, 'child') }.to raise_error
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
      it "returns a #{type}put port" do
        spec = {}
        spec[:"#{type}ports"] = {port: {}}
        @job = SideJob.queue('testq', 'TestWorker', **spec)
        expect(@job.send("#{type}put", :port)).to eq(SideJob::Port.new(@job, type, :port))
      end

      it 'raises error on unknown port' do
        @job = SideJob.queue('testq', 'TestWorker')
        expect { @job.send("#{type}put", :unknown) }.to raise_error
      end

      it 'can dynamically create ports' do
        spec = {}
        spec[:"#{type}ports"] = {'*' => {mode: :memory, default: 123}}
        @job = SideJob.queue('testq', 'TestWorker', **spec)
        expect(@job.send("#{type}ports").size).to eq 0
        port = @job.send("#{type}put", :newport)
        expect(@job.send("#{type}ports").size).to eq 1
        expect(port.mode).to eq :memory
        expect(port.default).to eq 123
      end
    end

    describe "##{type}ports" do
      it "returns all #{type}put ports" do
        @job = SideJob.queue('testq', 'TestWorker', inports: { port1: {} }, outports: { port1: {} })
        expect(@job.send("#{type}ports")).to eq([SideJob::Port.new(@job, type, :port1)])
      end
    end

    describe "##{type}ports=" do
      before do
        @job = SideJob.queue('testq', 'TestWorker')
      end

      it 'can specify ports with options' do
        expect(@job.send("#{type}ports").size).to eq 0
        @job.send("#{type}ports=", {myport: {mode: :memory, default: 'def'}})
        expect(@job.send("#{type}ports").size).to eq 1
        expect(@job.send("#{type}ports").map(&:name)).to include(:myport)
        expect(@job.send("#{type}put", :myport).mode).to eq :memory
        expect(@job.send("#{type}put", :myport).default).to eq 'def'
      end

      it 'merges ports with the worker configuration' do
        allow(SideJob::Worker).to receive(:config) { {"#{type}ports" => {'port1' => {}, 'port2' => {'mode' => 'memory'}}}}
        @job.send("#{type}ports=", {port2: {mode: :queue}, port3: {}})
        expect(@job.send("#{type}ports").size).to eq 3
        expect(@job.send("#{type}ports").all? {|port| port.options == {mode: :queue}}).to be true
      end

      it 'can change existing port mode while keeping data intact' do
        @job.send("#{type}ports=", {myport: {}})
        @job.send("#{type}put", :myport).write 'data'
        @job.send("#{type}ports=", {myport: {mode: :memory, default: 'def'}})
        expect(@job.send("#{type}ports").size).to eq 1
        expect(@job.send("#{type}put", :myport).mode).to eq :memory
        expect(@job.send("#{type}put", :myport).default).to eq 'def'
        expect(@job.send("#{type}put", :myport).read).to eq 'data'
        expect(@job.send("#{type}put", :myport).read).to eq 'def'
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

  describe '#state' do
    before do
      now = Time.now
      allow(Time).to receive(:now) { now }
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'returns job state with common and internal keys' do
      @job.set({abc: 123})
      expect(@job.state).to eq({"queue"=>"testq", "class"=>"TestWorker", "args"=>nil, "created_by"=>nil, "created_at"=>SideJob.timestamp, 'status' => 'queued', 'abc' => 123})
    end
  end

  describe '#get' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
      @job.set({field1: 'value1', field2: [1,2], field3: 123 })
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

    it 'always returns the latest value' do
      expect(@job.get(:field3)).to eq 123
      SideJob.redis.hmset @job.redis_key, :field3, '789'
      expect(@job.get(:field3)).to eq 789
    end
  end

  describe '#set' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'can save state in redis' do
      @job.set(test: 'data', test2: 123)
      state = @job.state
      expect(state['test']).to eq 'data'
      expect(state['test2']).to eq 123

      # test updating
      @job.set(test: 'data2')
      expect(@job.get('test')).to eq 'data2'
    end

    it 'can update values' do
      3.times do |i|
        @job.set key: [i]
        expect(@job.get(:key)).to eq [i]
        expect(JSON.parse(SideJob.redis.hget(@job.redis_key, 'key'))).to eq [i]
      end
    end

    it 'raises error if job no longer exists' do
      @job.status = 'terminated'
      SideJob.find(@job.id).delete
      expect { @job.set key: 123 }.to raise_error
    end
  end

  describe '#unset' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'unsets fields' do
      @job.set(a: 123, b: 456, c: 789)
      @job.unset('a', :b)
      expect(@job.get(:a)).to eq nil
      expect(@job.get(:b)).to eq nil
      expect(@job.get(:c)).to eq 789
    end
  end
end
