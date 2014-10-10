require 'spec_helper'

describe SideJob::Worker do
  before do
    @job = SideJob.queue('testq', 'TestWorker')
    set_status @job, 'running'
    @worker = TestWorker.new
    @worker.jid = @job.jid
  end

  describe '.register' do
    it 'adds a worker spec' do
      spec = {abc: [1, 2]}
      SideJob::Worker.register('testq', 'TestWorker', spec)
      expect(SideJob.redis.hget('workers:testq', 'TestWorker')).to eq(JSON.generate(spec))
    end
  end

  describe '.spec' do
    it 'returns a worker spec that has been registered' do
      expect(SideJob::Worker.spec('testq', 'TestWorker')).to be nil
      spec = {'abc' => [1, 2]}
      SideJob::Worker.register('testq', 'TestWorker', spec)
      expect(SideJob::Worker.spec('testq', 'TestWorker')).to eq spec
    end
  end

  describe '.unregister' do
    it 'unregisters a worker spec' do
      spec = {abc: [1, 2]}
      SideJob::Worker.register('testq', 'TestWorker', spec)
      expect(SideJob.redis.hget('workers:testq', 'TestWorker')).to eq(JSON.generate(spec))
      SideJob::Worker.unregister('testq', 'TestWorker')
      expect(SideJob.redis.hget('workers:testq', 'TestWorker')).to be nil
    end
  end

  describe '.unregister_all' do
    it 'unregisters all worker specs on a queue' do
      5.times {|i| SideJob::Worker.register('q1', "TestWorker#{i}", {worker: i}) }
      5.times {|i| SideJob::Worker.register('q2', "TestWorker#{i}", {worker: i}) }
      expect(SideJob.redis.hlen('workers:q1')).to eq 5
      expect(SideJob.redis.hlen('workers:q2')).to eq 5
      SideJob::Worker.unregister_all('q1')
      expect(SideJob.redis.hlen('workers:q1')).to eq 0
      expect(SideJob.redis.hlen('workers:q2')).to eq 5
    end
  end

  describe '.configure' do
    it 'raises error on unknown key' do
      expect { TestWorker.configure({unknown: 123}) }.to raise_error
    end

    it 'stores and retrieves alternate configuration' do
      expect(TestWorkerNoLog.configuration).to eq({log_status: false})
    end
  end

  it 'includes SideJob::JobMethods' do
    expect(TestWorker.included_modules).to include(SideJob::JobMethods)
  end

  it '#suspend raises exception' do
    expect { @worker.suspend }.to raise_error(SideJob::Worker::Suspended)
  end

  describe '#queue' do
    it 'can queue child jobs' do
      expect(SideJob).to receive(:queue).with('q2', 'TestWorker', args: [1,2], parent: @job, by: "job:#{@worker.jid}").and_call_original
      expect {
        child = @worker.queue('q2', 'TestWorker', args: [1, 2])
        expect(child.parent).to eq(@job)
        expect(@job.children).to eq([child])
      }.to change {Sidekiq::Stats.new.enqueued}.by(1)
    end

    it 'queues with by string set to self' do
      child = @worker.queue('q2', 'TestWorker')
      expect(child.by).to eq "job:#{@worker.jid}"
    end
  end

  describe '#find' do
    it 'calls SideJob.find with by string set to self' do
      job2 = SideJob.queue('testq', 'TestWorker')
      expect(@worker.find(job2.jid).by).to eq "job:#{@worker.jid}"
    end
  end

  describe '#get_config' do
    it 'returns nil if data is not available' do
      expect(@worker.get_config('field1')).to be nil
    end

    it 'can return false as a config value from port' do
      @worker.input(:field1).write false
      expect(@worker.get_config('field1')).to be false
    end

    it 'can return false as a config value from saved data' do
      @worker.set(field1: false)
      expect(@worker.get_config('field1')).to be false
    end

    it 'uses saved data if no data on input port' do
      @worker.set(field1: {field1: 'value1', field2: 123})
      expect(@worker.get_config('field1')).to eq({'field1' => 'value1', 'field2' => 123})
    end

    it 'uses input data if present' do
      @worker.set(field1: 'value1')
      @worker.input(:field1).write('value2')
      expect(@worker.get_config('field1')).to eq('value2')
      expect(@worker.get_config('field1')).to eq('value2')
      expect(@worker.get('field1')).to eq('value2')
    end

    it 'uses the latest input data when present' do
      @worker.set(field1: 'value1')
      @worker.input(:field1).write('value2')
      @worker.input(:field1).write('value3')
      expect(@worker.get_config('field1')).to eq('value3')
      expect(@worker.get_config('field1')).to eq('value3')
      expect(@worker.get('field1')).to eq('value3')
    end
  end
end
