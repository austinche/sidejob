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

  describe '#queue' do
    before do
      @job = SideJob.queue('testq', 'TestWorker', [1])
    end

    it 'can queue child jobs' do
      expect(SideJob).to receive(:queue).with('q2', 'TestWorker', [2], @job).and_call_original
      expect {
        child = @job.queue('q2', 'TestWorker', [2])
        expect(child.parent).to eq(@job)
        expect(@job.children).to eq([child])
      }.to change(TestWorker.jobs, :size).by(1)
    end
  end

  describe '#mset' do
    before do
      @job = SideJob.queue('testq', 'TestWorker', [])
    end

    it 'stores metadata in redis' do
      @job.mset({test: 'data'})
      expect(SideJob.redis {|conn| conn.hget(@job.jid, 'test')}).to eq('data')

      # test updating
      @job.mset({test: 'data2'})
      expect(SideJob.redis {|conn| conn.hget(@job.jid, 'test')}).to eq('data2')
    end

    it 'sets updated_at timestamp' do
      now = Time.now
      Time.stub(:now).and_return(now)
      @job.mset({})
      expect(SideJob.redis {|conn| conn.hget(@job.jid, 'updated_at')}).to eq(now.to_i.to_s)
    end
  end

  describe '#mget' do
    before do
      @job = SideJob.queue('testq', 'TestWorker', [])
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
      @job = SideJob.queue('testq', 'TestWorker', [])
    end

    it 'are shorthands for getting/setting single data fields' do
      @job.set('field1', 'value1')
      expect(@job.get('field1')).to eq('value1')
    end
  end

  describe '#status, #status=' do
    it 'store status as symbol and loads as symbol' do
      job = SideJob.queue('testq', 'TestWorker', [])
      job.status = :newstatus
      expect(SideJob::Job.new(job.jid).status).to eq(:newstatus)
    end

    it 'store status as string and loads as symbol' do
      job = SideJob.queue('testq', 'TestWorker', [])
      job.status = 'newstatus'
      expect(SideJob::Job.new(job.jid).status).to eq(:newstatus)
    end
  end

  describe '#children, #parent' do
    it 'can get children and parent jobs' do
      parent = SideJob.queue('testq', 'TestWorker', [])
      child = parent.queue('q2', 'TestWorker', [1])
      expect(TestWorker.jobs.size).to eq(2)
      expect(parent.children).to eq([child])
      expect(child.parent).to eq(parent)
    end
  end

  describe '.restart' do
    before do
      @job = SideJob.queue('testq', 'TestWorker', [1])
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

  describe '.delete' do
    before do
      @job = SideJob.queue('testq', 'TestWorker', [])
    end

    it 'recursively deletes jobs' do
      child = @job.queue('q2', 'TestWorker', [1])
      expect(@job.status).to eq(:queued)
      expect(child.status).to eq(:queued)
      expect(SideJob.redis {|conn| conn.keys('*').length}).to be > 0
      @job.delete
      expect(SideJob.redis {|conn| conn.keys('*').length}).to be(0)
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
      expect(SideJob.redis {|conn| conn.keys('*').length}).to be(0)
    end
  end

  describe '.input' do
    it 'returns an input port' do
      job = SideJob::Job.new('job')
      expect(job.input('port')).to eq(SideJob::Port.new(job, :in, 'port'))
    end
  end

  describe '.output' do
    it 'returns an output port' do
      job = SideJob::Job.new('job')
      expect(job.output('port')).to eq(SideJob::Port.new(job, :out, 'port'))
    end
  end

  describe '.inports' do
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

  describe '.outports' do
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
