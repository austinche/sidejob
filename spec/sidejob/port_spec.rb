require 'spec_helper'

describe SideJob::Port do
  before do
    @job = SideJob::Job.new('job')
    @port = SideJob::Port.new(@job, :in, :port1)
  end

  describe '#initialize' do
    it 'raises error if name is invalid' do
      expect { SideJob::Port.new(@job, :in, 'PORT.1')}.to raise_error
    end

    it 'raises error if name is empty' do
      expect { SideJob::Port.new(@job, :in, '')}.to raise_error
    end
  end

  describe '#==, #eql?' do
    it 'two ports with the same job, type, and name are eq' do
      expect(SideJob::Port.new(@job, :in, :port1)).to eq(@port)
      expect(SideJob::Port.new(@job, :in, 'port1')).to eq(@port)
      expect(SideJob::Port.new(@job, :in, 'port1')).to eql(@port)
    end

    it 'two ports with different names are not eq' do
      expect(SideJob::Port.new(@job, :in, 'port2')).not_to eq(@port)
      expect(SideJob::Port.new(@job, :in, 'port2')).not_to eql(@port)
    end

    it 'port names are case sensitive' do
      expect(SideJob::Port.new(@job, :in, 'PORT1')).not_to eq(@port)
      expect(SideJob::Port.new(@job, :in, 'PORT1')).not_to eql(@port)
    end
  end

  describe '#size' do
    it 'returns 0 when the port is empty' do
      expect(@port.size).to eq 0
    end

    it 'number of elements is increased on push' do
      expect {
        @port.push 1
      }.to change(@port, :size).by(1)
    end

    it 'number of elements is decreased on pop' do
      @port.push 1
      expect {
        @port.pop
      }.to change(@port, :size).by(-1)
    end
  end

  describe '#push' do
    it 'can push data to a port' do
      @port.push('abc', 123)
      data = SideJob.redis { |redis| redis.lrange(@port.redis_key, 0, -1) }
      expect(data).to eq(['123', 'abc'])
    end

    it 'logs pushes' do
      now = Time.now
      Time.stub(:now).and_return(now)
      expect(@job.log_pop).to be nil
      @port.push('abc', '123')
      expect(@job.log_pop).to eq({'type' => 'write', 'inport' => 'port1', 'data' => 'abc', 'timestamp' => now.to_s})
      expect(@job.log_pop).to eq({'type' => 'write', 'inport' => 'port1', 'data' => '123', 'timestamp' => now.to_s})
      expect(@job.log_pop).to be nil
    end

    it 'restarts job when pushing to an input port' do
      @job.status = :running
      inport = SideJob::Port.new(@job, :in, :port1)
      expect(@job.restarting?).to be false
      inport.push('abc')
      expect(@job.restarting?).to be true
    end

    it 'restarts parent job when pushing to an output port' do
      @parent = SideJob::Job.new('job')
      @job.parent = @parent
      @parent.status = :running
      outport = SideJob::Port.new(@job, :out, :port1)
      expect(@parent.restarting?).to be false
      outport.push('abc')
      expect(@parent.restarting?).to be true
    end
  end

  describe '#pop' do
    it 'can pop data from a port' do
      expect(@port.pop).to be_nil
      @port.push('abc', 123, JSON.generate(['data1', 1, {key: 'val'}]))
      expect(@port.size).to be(3)
      expect(@port.pop).to eq('abc')
      expect(@port.pop).to eq('123')
      expect(JSON.parse(@port.pop)).to eq(['data1', 1, {'key' => 'val'}])
      expect(@port.pop).to be_nil
      expect(@port.size).to be(0)
    end

    it 'logs pops' do
      now = Time.now
      Time.stub(:now).and_return(now)
      @port.push('abc', '123')
      while @job.log_pop; end
      expect(@port.pop).to eq('abc')
      expect(@port.pop).to eq('123')
      expect(@job.log_pop).to eq({'type' => 'read', 'inport' => 'port1', 'data' => 'abc', 'timestamp' => now.to_s})
      expect(@job.log_pop).to eq({'type' => 'read', 'inport' => 'port1', 'data' => '123', 'timestamp' => now.to_s})
      expect(@job.log_pop).to be nil
    end
  end

  describe '#pop_all' do
    it 'returns empty array when port is empty' do
      expect(@port.pop_all).to eq([])
    end

    it 'returns array with most recent items first' do
      @port.push '1'
      @port.push '2'
      @port.push '3'
      expect(@port.pop_all).to eq(['3', '2', '1'])
      expect(@port.pop_all).to eq([])
      expect(@port.pop).to be nil
    end
  end

  describe '#push_json, #pop_json, #pop_all_json' do
    it 'encodes/decodes from JSON' do
      expect(@port.pop_json).to be_nil
      data1 = ['data1', 1, {'key' => 'val'}]
      data2 = {'abc' => 123}
      @port.push_json(data1, data2)
      expect(@port.pop_json).to eq(data1)
      expect(@port.pop_json).to eq(data2)
      @port.push_json(data1, data2)
      expect(@port.pop_all_json).to eq([data2, data1])
    end
  end

  describe '#redis_key' do
    it 'returns key with valid name' do
      expect(@port.redis_key).to eq("#{@port.job.redis_key}:in:port1")
    end
  end

  describe '#hash' do
    it 'uses hash of the redis key' do
      expect(@port.hash).to eq(@port.redis_key.hash)
    end

    it 'can be used as keys in a hash' do
      h = {}
      h[@port] = 1
      port2 = SideJob::Port.new(@job, :in, 'port1')
      expect(@port.hash).to eq(port2.hash)
      h[port2] = 3
      expect(h.keys.length).to be(1)
      expect(h[@port]).to be(3)
    end
  end

  describe '.all' do
    it 'returns ports that have been pushed to but not popped from' do
      expect(SideJob::Port.all(@job, :in)).to eq([])
      @port.push 'abc'
      expect(SideJob::Port.all(@job, :in)).to match_array([@port])
      port2 = SideJob::Port.new(@job, :in, 'port2')
      port2.pop
      expect(SideJob::Port.all(@job, :in)).to match_array([@port])
      port2.push '123'
      expect(SideJob::Port.all(@job, :in)).to match_array([@port, port2])
    end
  end

  describe '.delete_all' do
    it 'delete all port keys' do
      expect(SideJob.redis {|redis| redis.keys("#{@port.redis_key}*").length}).to be(0)
      @port.push 'abc'
      keys = SideJob.redis {|redis| redis.keys("#{@port.redis_key}*").length}
      SideJob::Port.new(@job, :out, 'port2').push 'abc'
      SideJob::Port.new(@job, :out, 'port3').push 'abc'
      SideJob::Port.delete_all(@job, :out)
      expect(SideJob.redis {|redis| redis.keys("#{@port.redis_key}*").length}).to be(keys)
      SideJob::Port.delete_all(@job, :in)
      expect(SideJob.redis {|redis| redis.keys("#{@port.redis_key}*").length}).to be(0)
    end
  end
end
