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

  it 'includes SideJob::JobMethods' do
    expect(TestWorker.included_modules).to include(SideJob::JobMethods)
  end

  it '#suspend raises exception' do
    expect { @worker.suspend }.to raise_error(SideJob::Worker::Suspended)
  end

  describe '#queue' do
    it 'can queue child jobs' do
      expect(SideJob).to receive(:queue).with('q2', 'TestWorker', args: [1,2], parent: @job).and_call_original
      expect {
        child = @worker.queue('q2', 'TestWorker', args: [1, 2])
        expect(child.parent).to eq(@job)
        expect(@job.children).to eq([child])
      }.to change {Sidekiq::Stats.new.enqueued}.by(1)
    end
  end

  describe '#get_config' do
    it 'returns nil if data is not available' do
      expect(@worker.get_config('field1')).to be nil
    end

    it 'uses saved data if no data on input port' do
      @worker.set('field1', 'value1')
      expect(@worker.get_config('field1')).to eq('value1')
    end

    it 'uses input data if present' do
      @worker.set('config:field1', 'value1')
      @worker.input(:field1).write('value2')
      expect(@worker.get_config('field1')).to eq('value2')
      expect(@worker.get_config('field1')).to eq('value2')
      expect(@worker.get('field1')).to eq('value2')
    end

    it 'uses the latest input data when present' do
      @worker.set('field1', 'value1')
      @worker.input(:field1).write('value2')
      @worker.input(:field1).write('value3')
      expect(@worker.get_config('field1')).to eq('value3')
      expect(@worker.get_config('field1')).to eq('value3')
      expect(@worker.get('field1')).to eq('value3')
    end
  end

  describe '#get_config_json' do
    it 'returns nil if data is not available' do
      expect(@worker.get_config_json ('field1')).to be nil
    end

    it 'uses saved data if no data on input port' do
      val = {'val' => 1}
      @worker.set_json('field1', val)
      expect(@worker.get_config_json('field1')).to eq(val)
    end

    it 'uses input data if present' do
      val = {'val' => 2}
      @worker.set_json('field1', {'val' => 1})
      @worker.input(:field1).write_json(val)
      expect(@worker.get_config_json('field1')).to eq(val)
      expect(@worker.get_config_json('field1')).to eq(val)
      expect(@worker.get_json('field1')).to eq(val)
    end

    it 'uses the latest input data when present' do
      val = {'val' => 3}
      @worker.set_json('field1', {'val' => 1})
      @worker.input(:field1).write_json({'val' => 2})
      @worker.input(:field1).write_json(val)
      expect(@worker.get_config_json('field1')).to eq(val)
      expect(@worker.get_config_json('field1')).to eq(val)
      expect(@worker.get_json('field1')).to eq(val)
    end
  end
end
