require 'spec_helper'

describe SideJob::Job do
  describe '#==, #eql?' do
    it 'two jobs with the same jid are eq' do
      expect(SideJob::Job.new('123')).to eq(SideJob::Job.new('123'))
      expect(SideJob::Job.new('123')).to eql(SideJob::Job.new('123'))
    end

    it 'two jobs with different jid are not eq' do
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

  describe '#exists?' do
    it 'returns true if job exists' do
      @job = SideJob.queue('testq', 'TestWorker')
      expect(@job.exists?).to be true
    end
    it 'returns false if job does not exist' do
      expect(SideJob::Job.new('job').exists?).to be false
    end
  end

  describe '#info' do
    it 'returns all job info' do
      now = Time.now
      Time.stub(:now).and_return(now)
      @job = SideJob.queue('testq', 'TestWorker', {args: [1, 2]})
      expect(@job.info).to eq({queue: 'testq', class: 'TestWorker', args: [1, 2], status: 'queued',
                               created_by: '', created_at: SideJob.timestamp, updated_at: SideJob.timestamp, ran_at: nil })
    end
  end

  describe '#log' do
    it 'adds a timestamp to log entries' do
      now = Time.now
      Time.stub(:now).and_return(now)
      job = SideJob.queue('testq', 'TestWorker')
      job.log('foo', {abc: 123})
      log = SideJob.redis.lpop "#{job.redis_key}:log"
      expect(JSON.parse(log)).to eq({'type' => 'foo', 'abc' => 123, 'timestamp' => SideJob.timestamp})
    end

    it 'updates updated_at timestamp' do
      now = Time.now
      Time.stub(:now).and_return(now)
      job = SideJob.queue('testq', 'TestWorker')
      job.log('foo', {abc: 123})
      expect(job.info[:updated_at]).to eq(SideJob.timestamp)
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
      SideJob.redis.hset @job.redis_key, 'status', 'newstatus'
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
      set_status @job, 'terminated'
      @job.terminate
      expect(@job.status).to eq 'terminated'
    end

    it 'queues the job for termination run' do
      expect {
        @job.terminate
      }.to change {Sidekiq::Stats.new.enqueued}.by(1)
    end

    it 'by default does not terminate children' do
      child = SideJob.queue('q2', 'TestWorker', parent: @job)
      expect(child.status).to eq 'queued'
      @job.terminate
      expect(child.status).to eq 'queued'
    end

    it 'can recursively terminate' do
      5.times { SideJob.queue('q2', 'TestWorker', parent: @job) }
      @job.terminate(recursive: true)
      @job.children.each do |child|
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
          set_status @job, status
          @job.run
          expect(@job.status).to eq 'queued'
        }.to change {Sidekiq::Stats.new.enqueued}.by(1)
      end
    end

    %w{terminating terminated}.each do |status|
      it "does not queue the job if status is #{status}" do
        expect {
          set_status @job, status
          @job.run
          expect(@job.status).to eq status
        }.to change {Sidekiq::Stats.new.enqueued}.by(0)
      end

      it "queues the job if status is #{status} and force=true" do
        expect {
          set_status @job, status
          @job.run(force: true)
          expect(@job.status).to eq 'queued'
        }.to change {Sidekiq::Stats.new.enqueued}.by(1)
      end
    end

    it 'can schedule a job to run at a specific time using a float' do
      time = Time.now.to_f + 10000
      expect { @job.run(at: time) }.to change {Sidekiq::Stats.new.scheduled_size}.by(1)
      expect(Sidekiq::ScheduledSet.new.find_job(@job.jid).at).to eq(Time.at(time))
      expect(@job.status).to eq 'queued'
    end

    it 'can schedule a job to run at a specific time using a Time' do
      time = Time.now + 1000
      expect { @job.run(at: time) }.to change {Sidekiq::Stats.new.scheduled_size}.by(1)
      expect(Sidekiq::ScheduledSet.new.find_job(@job.jid).at).to eq(Time.at(time.to_f))
      expect(@job.status).to eq 'queued'
    end

    it 'can schedule a job to run in a specific time' do
      now = Time.now
      Time.stub(:now).and_return(now)
      expect { @job.run(wait: 100) }.to change {Sidekiq::Stats.new.scheduled_size}.by(1)
      expect(Sidekiq::ScheduledSet.new.find_job(@job.jid).at).to eq(Time.at(now.to_f + 100))
      expect(@job.status).to eq 'queued'
    end
  end

  describe '#children, #parent' do
    it 'can get children and parent jobs' do
      parent = SideJob.queue('testq', 'TestWorker')
      child = SideJob.queue('q2', 'TestWorker', {parent: parent})
      expect(parent.children).to eq([child])
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
      j2 = SideJob.queue('q2', 'TestWorker', {parent: j1})
      j3 = SideJob.queue('q2', 'TestWorker', {parent: j2})
      j4 = SideJob.queue('q2', 'TestWorker', {parent: j3})
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
      set_status @job, 'terminated'
      expect(@job.terminated?).to be true
    end

    it 'returns false if child job is not terminated' do
      set_status @job, 'terminated'
      SideJob.queue('q', 'TestWorker', parent: @job)
      expect(@job.terminated?).to be false
    end

    it 'returns true if child job is terminated' do
      set_status @job, 'terminated'
      child = SideJob.queue('q', 'TestWorker', parent: @job)
      set_status child, 'terminated'
      expect(@job.terminated?).to be true
    end
  end

  describe '#delete' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'does not delete non-terminated jobs' do
      expect(@job.delete).to be false
      expect(@job.exists?).to be true
    end

    it 'deletes terminated jobs' do
      set_status @job, 'terminated'
      expect(@job.delete).to be true
      expect(@job.exists?).to be false
    end

    it 'recursively deletes jobs' do
      child = SideJob.queue('q2', 'TestWorker', {parent: @job})
      expect(@job.status).to eq('queued')
      expect(child.status).to eq('queued')
      expect(SideJob.redis {|redis| redis.keys('job:*').length}).to be > 0
      set_status @job, 'terminated'
      set_status child, 'terminated'
      @job.delete
      expect(SideJob.redis {|redis| redis.keys('job:*').length}).to be(0)
      expect(@job.status).to be_nil
      expect(child.status).to be_nil
    end

    it 'deletes data on input and output ports' do
      @job.input('port1').write 'data'
      @job.output('port2').write 'data'
      expect(@job.inports).to eq([@job.input('port1')])
      expect(@job.outports).to eq([@job.output('port2')])
      set_status @job, 'terminated'
      @job.delete
      expect(@job.inports).to eq([])
      expect(@job.outports).to eq([])
      expect(@job.input('port1').read).to be_nil
      expect(@job.output('port2').read).to be_nil
      expect(SideJob.redis {|redis| redis.keys('job:*').length}).to be(0)
    end

    it 'removes job from jobs set' do
      expect(SideJob.redis {|redis| redis.sismember('jobs', @job.jid)}).to be true
      set_status @job, 'terminated'
      @job.delete
      expect(SideJob.redis {|redis| redis.sismember('jobs', @job.jid)}).to be false
    end
  end

  describe '#input' do
    it 'returns an input port' do
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.input('port')).to eq(SideJob::Port.new(job, :in, 'port'))
    end
  end

  describe '#output' do
    it 'returns an output port' do
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.output('port')).to eq(SideJob::Port.new(job, :out, 'port'))
    end
  end

  describe '#inports' do
    it 'returns input ports that have data' do
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.inports).to eq([])
      job.input('port1').write 'abc'
      expect(job.inports).to eq([SideJob::Port.new(job, :in, 'port1')])
      job.input('port2').read
      expect(job.inports).to eq([SideJob::Port.new(job, :in, 'port1')])
      job.input('port2').write 'abc'
      expect(job.inports).to match_array([SideJob::Port.new(job, :in, 'port1'), SideJob::Port.new(job, :in, 'port2')])
      job.input('port1').read
      expect(job.inports).to eq([SideJob::Port.new(job, :in, 'port2')])
    end
  end

  describe '#outports' do
    it 'returns output ports that have data' do
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.outports).to eq([])
      job.output('port1').write 'abc'
      expect(job.outports).to eq([SideJob::Port.new(job, :out, 'port1')])
      job.output('port2').read
      expect(job.outports).to eq([SideJob::Port.new(job, :out, 'port1')])
      job.output('port2').write 'abc'
      expect(job.outports).to match_array([SideJob::Port.new(job, :out, 'port1'), SideJob::Port.new(job, :out, 'port2')])
      job.output('port1').read
      expect(job.outports).to eq([SideJob::Port.new(job, :out, 'port2')])
    end
  end

  describe '#set' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'stores metadata in redis' do
      @job.set(test: 'data', test2: 123)
      expect(SideJob.redis.hget("#{@job.redis_key}:data", 'test')).to eq '"data"'
      expect(SideJob.redis.hget("#{@job.redis_key}:data", 'test2')).to eq '123'

      # test updating
      @job.set(test: 'data2')
      expect(SideJob.redis.hget("#{@job.redis_key}:data", 'test')).to eq('"data2"')
    end

    it 'updates updated_at timestamp' do
      now = Time.now + 1000
      Time.stub(:now).and_return(now)
      @job.set({test: 123})
      expect(@job.info[:updated_at]).to eq(SideJob.timestamp)
    end
  end

  describe '#get' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
      @data = { field1: 'value1', field2: 'value2', field3: 123 }
      @job.set @data
    end

    it 'returns single value if only one field specified' do
      expect(@job.get(:field3)).to eq 123
    end

    it 'returns nil for missing value' do
      expect(@job.get('missing')).to be nil
    end

    it 'only loads specified fields' do
      expect(@job.get(:field1, :field3)).to eq({field1: 'value1', field3: 123})
    end

    it 'returns String or Symbol depending on passed in field' do
      data = @job.get(:field1, 'field2')
      expect(data[:field1]).to eq('value1')
      expect(data['field2']).to eq('value2')
    end

    it 'loads all fields if none specified' do
      data = @job.get
      expect(data['field1']).to eq('value1')
      expect(data['field2']).to eq('value2')
      expect(data['field3']).to eq(123)
    end

    it 'can store and retrieve complex objects in metadata' do
      data = {'nested' => [1, 'b', {'foo' => nil}]}
      @job.set(test: data)
      expect(@job.get(:test)).to eq data
    end
  end

  describe '#unset' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end
    it 'unsets fields' do
      @job.set(a: 123, b: 456, c: 789)
      @job.unset('a', :b)
      expect(@job.get).to eq({'c' => 789})
    end
  end

  describe '#touch' do
    it 'updates the updated_at timestamp' do
      now = Time.now
      Time.stub(:now).and_return(now)
      @job = SideJob.queue('testq', 'TestWorker')
      expect(@job.info[:updated_at]).to eq(SideJob.timestamp)
      now = Time.now + 1000
      Time.stub(:now).and_return(now)
      @job.touch
      expect(@job.info[:updated_at]).to eq(SideJob.timestamp)
    end
  end
end
