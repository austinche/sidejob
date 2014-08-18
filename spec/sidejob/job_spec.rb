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

  describe '#queue' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'can queue child jobs' do
      expect(SideJob).to receive(:queue).with('q2', 'TestWorker', {args: [1,2]}).and_call_original
      expect {
        child = @job.queue('q2', 'TestWorker', {args: [1, 2]})
        expect(child.parent).to eq(@job)
        expect(@job.children).to eq([child])
      }.to change(TestWorker.jobs, :size).by(1)
    end
  end

  describe '#mset' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'stores metadata in redis' do
      @job.mset({test: 'data'})
      expect(SideJob.redis {|redis| redis.hget("#{@job.redis_key}:data", 'test')}).to eq('data')

      # test updating
      @job.mset({test: 'data2'})
      expect(SideJob.redis {|redis| redis.hget("#{@job.redis_key}:data", 'test')}).to eq('data2')
    end
  end

  describe '#mget' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'only loads specified fields' do
      data = { field1: 'value1', field2: 'value2' }
      @job.mset data
      expect(@job.mget(:field1, :field2)).to eq(data)
    end

    it 'returns String or Symbol depending on passed in field' do
      data = { field1: 'value1', field2: 'value2' }
      @job.mset data
      data = @job.mget(:field1, 'field2')
      expect(data[:field1]).to eq('value1')
      expect(data['field2']).to eq('value2')
    end

    it 'loads all fields if none specified' do
      data = { field1: 'value1', field2: 'value2' }
      @job.mset data
      data = @job.mget
      expect(data['field1']).to eq('value1')
      expect(data['field2']).to eq('value2')
    end
  end

  describe '#get, #set' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'are shorthands for getting/setting single data fields' do
      @job.set('field1', 'value1')
      expect(@job.get('field1')).to eq('value1')
    end
  end

  describe '#get_json, #set_json' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'can be used to store objects as json' do
      data = {'abc' => 123, 'def' => [1, 2]}
      @job.set_json(:field1, data)
      expect(@job.get_json(:field1)).to eq(data)
    end
  end

  describe '#get_config' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'returns nil if data is not available' do
      expect(@job.get_config('field1')).to be nil
    end

    it 'uses saved data if no data on input port' do
      @job.set('field1', 'value1')
      expect(@job.get_config('field1')).to eq('value1')
    end

    it 'uses input data if present' do
      @job.set('config:field1', 'value1')
      @job.input(:field1).push('value2')
      expect(@job.get_config('field1')).to eq('value2')
      expect(@job.get_config('field1')).to eq('value2')
      expect(@job.get('field1')).to eq('value2')
    end

    it 'uses the latest input data when present' do
      @job.set('field1', 'value1')
      @job.input(:field1).push('value2')
      @job.input(:field1).push('value3')
      expect(@job.get_config('field1')).to eq('value3')
      expect(@job.get_config('field1')).to eq('value3')
      expect(@job.get('field1')).to eq('value3')
    end
  end

  describe '#get_config_json' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'returns nil if data is not available' do
      expect(@job.get_config_json ('field1')).to be nil
    end

    it 'uses saved data if no data on input port' do
      val = {'val' => 1}
      @job.set_json('field1', val)
      expect(@job.get_config_json('field1')).to eq(val)
    end

    it 'uses input data if present' do
      val = {'val' => 2}
      @job.set_json('field1', {'val' => 1})
      @job.input(:field1).push_json(val)
      expect(@job.get_config_json('field1')).to eq(val)
      expect(@job.get_config_json('field1')).to eq(val)
      expect(@job.get_json('field1')).to eq(val)
    end

    it 'uses the latest input data when present' do
      val = {'val' => 3}
      @job.set_json('field1', {'val' => 1})
      @job.input(:field1).push_json({'val' => 2})
      @job.input(:field1).push_json(val)
      expect(@job.get_config_json('field1')).to eq(val)
      expect(@job.get_config_json('field1')).to eq(val)
      expect(@job.get_json('field1')).to eq(val)
    end
  end

  describe '#log_push, #log_pop' do
    it 'adds a timestamp to log entries' do
      now = Time.now
      Time.stub(:now).and_return(now)
      job = SideJob.queue('testq', 'TestWorker')
      while job.log_pop; end
      job.log_push('foo', {abc: 123})
      expect(job.log_pop).to eq({'type' => 'foo', 'abc' => 123, 'timestamp' => now.to_s})
    end
  end

  describe '#status, #status=' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'store status as symbol and loads as symbol' do
      @job.status = :newstatus
      expect(SideJob::Job.new(@job.jid).status).to eq(:newstatus)
    end

    it 'store status as string and loads as symbol' do
      @job.status = 'newstatus'
      expect(SideJob::Job.new(@job.jid).status).to eq(:newstatus)
    end

    it 'logs status changes' do
      now = Time.now
      Time.stub(:now).and_return(now)
      while @job.log_pop; end
      @job.status = 'newstatus'
      expect(@job.log_pop).to eq({'type' => 'status', 'status' => 'newstatus', 'timestamp' => now.to_s})
    end
  end

  describe '#children, #parent' do
    it 'can get children and parent jobs' do
      parent = SideJob.queue('testq', 'TestWorker')
      child = parent.queue('q2', 'TestWorker')
      expect(TestWorker.jobs.size).to eq(2)
      expect(parent.children).to eq([child])
      expect(child.parent).to eq(parent)
    end
  end

  describe '#parent=' do
    it 'raises error if parent is already set' do
      parent = SideJob.queue('testq', 'TestWorker')
      child = SideJob.queue('testq', 'TestWorker')
      parent2 = SideJob.queue('testq', 'TestWorker')
      child.parent = parent
      expect { child.parent = parent2 }.to raise_error
    end
  end

  describe '#tree' do
    it 'recursively gets job tree' do
      job1 = SideJob.queue('q', 'TestWorker')
      job2 = job1.queue('q', 'TestWorker')
      job3 = job1.queue('q', 'TestWorker')
      job4 = job2.queue('q', 'TestWorker')
      job5 = job4.queue('q', 'TestWorker')
      expect(job1.tree).to match_array([{job: job2, children: [{job: job4, children: [{job: job5, children: []}]}]}, {job: job3, children: []}])
    end
  end

  describe '#restart' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'does nothing on a queued job' do
      expect(@job.status).to eq(:queued)
      expect { @job.restart }.to change(TestWorker.jobs, :size).by(0)
      expect(@job.status).to eq(:queued)
    end

    it 'restarts a completed job' do
      @job.status = :completed
      expect { @job.restart }.to change(TestWorker.jobs, :size).by(1)
      expect(@job.status).to eq(:queued)
    end

    it 'restarts a suspended job' do
      @job.status = :suspended
      expect { @job.restart }.to change(TestWorker.jobs, :size).by(1)
      expect(@job.status).to eq(:queued)
    end
  end

  describe '#restarting?' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
      @job.status = :running
    end

    it 'returns false if not restarting' do
      expect(@job.restarting?).to be false
    end

    it 'returns true if restart called' do
      @job.restart
      expect(@job.restarting?).to be true
    end
  end

  describe '#delete' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'recursively deletes jobs' do
      child = @job.queue('q2', 'TestWorker')
      expect(@job.status).to eq(:queued)
      expect(child.status).to eq(:queued)
      expect(SideJob.redis {|redis| redis.keys('*').length}).to be > 0
      @job.delete
      expect(SideJob.redis {|redis| redis.keys('*').length}).to be(0)
      expect(@job.status).to be_nil
      expect(child.status).to be_nil
    end

    it 'deletes data on input and output ports' do
      @job.input('port1').push 'data'
      @job.output('port2').push 'data'
      expect(@job.inports).to eq([@job.input('port1')])
      expect(@job.outports).to eq([@job.output('port2')])
      @job.delete
      expect(@job.inports).to eq([])
      expect(@job.outports).to eq([])
      expect(@job.input('port1').pop).to be_nil
      expect(@job.output('port2').pop).to be_nil
      expect(SideJob.redis {|redis| redis.keys('*').length}).to be(0)
    end
  end

  describe '#input' do
    it 'returns an input port' do
      job = SideJob::Job.new('job')
      expect(job.input('port')).to eq(SideJob::Port.new(job, :in, 'port'))
    end
  end

  describe '#output' do
    it 'returns an output port' do
      job = SideJob::Job.new('job')
      expect(job.output('port')).to eq(SideJob::Port.new(job, :out, 'port'))
    end
  end

  describe '#inports' do
    it 'returns input ports that have been pushed to' do
      job = SideJob::Job.new('job')
      expect(job.inports.size).to be(0)
      job.input('port1').push 'abc'
      expect(job.inports.size).to be(1)
      expect(job.inports[0].name).to eq 'port1'
      job.input('port2').pop
      expect(job.inports.size).to be(1)
      job.input('port2').push 'abc'
      expect(job.inports.size).to be(2)
    end
  end

  describe '#outports' do
    it 'returns output ports that have been pushed to' do
      job = SideJob::Job.new('job')
      expect(job.outports.size).to be(0)
      job.output('port1').push 'abc'
      expect(job.outports.size).to be(1)
      expect(job.outports[0].name).to eq 'port1'
      job.output('port2').pop
      expect(job.outports.size).to be(1)
      job.output('port2').push 'abc'
      expect(job.outports.size).to be(2)
    end
  end
end
