require 'spec_helper'

describe SideJob::Worker do
  before do
    @job = SideJob.queue('testq', 'TestWorker')
    @job.status = :running
    @worker = TestWorker.new
    @worker.jid = @job.jid
  end

  it 'includes SideJob::JobMethods' do
    expect(TestWorker.included_modules).to include(SideJob::JobMethods)
  end

  it '#suspend sets status to suspended' do
    @worker.suspend
    expect(@worker.status).to eq(:suspended)
  end

  describe '#queue' do
    it 'can queue child jobs' do
      expect(SideJob).to receive(:queue).with('q2', 'TestWorker', {args: [1,2], parent: @job}).and_call_original
      expect {
        child = @worker.queue('q2', 'TestWorker', {args: [1, 2]})
        expect(child.parent).to eq(@job)
        expect(@job.children).to eq([child])
      }.to change(TestWorker.jobs, :size).by(1)
    end
  end

  describe '#mset' do
    it 'stores metadata in redis' do
      @worker.mset({test: 'data'})
      expect(SideJob.redis {|redis| redis.hget("#{@worker.redis_key}:data", 'test')}).to eq('data')

      # test updating
      @worker.mset({test: 'data2'})
      expect(SideJob.redis {|redis| redis.hget("#{@worker.redis_key}:data", 'test')}).to eq('data2')
    end
  end

  describe '#mget' do
    it 'only loads specified fields' do
      data = { field1: 'value1', field2: 'value2' }
      @worker.mset data
      expect(@worker.mget(:field1, :field2)).to eq(data)
    end

    it 'returns String or Symbol depending on passed in field' do
      data = { field1: 'value1', field2: 'value2' }
      @worker.mset data
      data = @worker.mget(:field1, 'field2')
      expect(data[:field1]).to eq('value1')
      expect(data['field2']).to eq('value2')
    end

    it 'loads all fields if none specified' do
      data = { field1: 'value1', field2: 'value2' }
      @worker.mset data
      data = @worker.mget
      expect(data['field1']).to eq('value1')
      expect(data['field2']).to eq('value2')
    end
  end

  describe '#get, #set' do
    it 'are shorthands for getting/setting single data fields' do
      @worker.set('field1', 'value1')
      expect(@worker.get('field1')).to eq('value1')
    end
  end

  describe '#get_json, #set_json' do
    it 'can be used to store objects as json' do
      data = {'abc' => 123, 'def' => [1, 2]}
      @worker.set_json(:field1, data)
      expect(@worker.get_json(:field1)).to eq(data)
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
