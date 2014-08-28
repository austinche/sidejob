require 'spec_helper'

describe SideJob::Port do
  before do
    @job = SideJob.queue('testq', 'TestWorker')
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

    it 'number of elements is increased on write' do
      expect {
        @port.write 1
      }.to change(@port, :size).by(1)
    end

    it 'number of elements is decreased on read' do
      @port.write 1
      expect {
        @port.read
      }.to change(@port, :size).by(-1)
    end
  end

  describe '#write' do
    it 'can write data to a port' do
      @port.write('abc', 123)
      data = SideJob.redis { |redis| redis.lrange(@port.redis_key, 0, -1) }
      expect(data).to eq(['123', 'abc'])
    end

    it 'logs writes' do
      now = Time.now
      Time.stub(:now).and_return(now)
      while @job.log_pop; end
      @port.write('abc', '123')
      expect(@job.log_pop).to eq({'type' => 'write', 'inport' => 'port1', 'data' => 'abc', 'timestamp' => now.to_s})
      expect(@job.log_pop).to eq({'type' => 'write', 'inport' => 'port1', 'data' => '123', 'timestamp' => now.to_s})
      expect(@job.log_pop).to be nil
    end

    it 'restarts job when writing to an input port' do
      @job.status = :running
      inport = SideJob::Port.new(@job, :in, :port1)
      expect(@job.restarting?).to be false
      inport.write('abc')
      expect(@job.restarting?).to be true
    end

    it 'restarts parent job when writing to an output port' do
      child = SideJob.queue('q', 'TestWorker', {parent: @job})
      @job.status = :running
      outport = SideJob::Port.new(child, :out, :port1)
      expect(@job.restarting?).to be false
      outport.write('abc')
      expect(@job.restarting?).to be true
    end
  end

  describe '#write_json' do
    it 'JSON encodes data before writing' do
      data1 = [1, 2]
      data2 = {abc: 123}
      expect(@port).to receive(:write).with(JSON.generate(data1), JSON.generate(data2))
      @port.write_json(data1, data2)
    end
  end

  describe '#read' do
    it 'can read data from a port' do
      expect(@port.read).to be_nil
      @port.write('abc', 123, JSON.generate(['data1', 1, {key: 'val'}]))
      expect(@port.size).to be(3)
      expect(@port.read).to eq('abc')
      expect(@port.read).to eq('123')
      expect(JSON.parse(@port.read)).to eq(['data1', 1, {'key' => 'val'}])
      expect(@port.read).to be_nil
      expect(@port.size).to be(0)
    end

    it 'logs reads' do
      now = Time.now
      Time.stub(:now).and_return(now)
      @port.write('abc', '123')
      while @job.log_pop; end
      expect(@port.read).to eq('abc')
      expect(@port.read).to eq('123')
      expect(@job.log_pop).to eq({'type' => 'read', 'inport' => 'port1', 'data' => 'abc', 'timestamp' => now.to_s})
      expect(@job.log_pop).to eq({'type' => 'read', 'inport' => 'port1', 'data' => '123', 'timestamp' => now.to_s})
      expect(@job.log_pop).to be nil
    end
  end

  describe '#read_json' do
    it 'JSON decodes read data' do
      expect(@port.read_json).to be_nil
      data1 = ['data1', 1, {'key' => 'val'}]
      data2 = {'abc' => 123}
      @port.write_json(data1, data2)
      expect(@port.read_json).to eq(data1)
      expect(@port.read_json).to eq(data2)
    end
  end
  describe '#drain' do
    it 'returns empty array when port is empty' do
      expect(@port.drain).to eq([])
    end

    it 'returns array with most recent items first' do
      @port.write '1'
      @port.write '2', '3'
      expect(@port.drain).to eq(['3', '2', '1'])
      expect(@port.drain).to eq([])
      expect(@port.read).to be nil
    end
  end

  describe '#drain_json' do
    it 'drains and JSON decodes data on port' do
      data1 = ['data1', 1, {'key' => 'val'}]
      data2 = {'abc' => 123}
      @port.write_json(data1, data2)
      expect(@port.drain_json).to eq([data2, data1])
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

  describe '.delete_all' do
    it 'delete all port keys' do
      expect(SideJob.redis {|redis| redis.keys("#{@port.redis_key}*").length}).to be(0)
      @port.write 'abc'
      keys = SideJob.redis {|redis| redis.keys("#{@port.redis_key}*").length}
      SideJob::Port.new(@job, :out, 'port2').write 'abc'
      SideJob::Port.new(@job, :out, 'port3').write 'abc'
      SideJob::Port.delete_all(@job, :out)
      expect(SideJob.redis {|redis| redis.keys("#{@port.redis_key}*").length}).to be(keys)
      SideJob::Port.delete_all(@job, :in)
      expect(SideJob.redis {|redis| redis.keys("#{@port.redis_key}*").length}).to be(0)
    end
  end
end
