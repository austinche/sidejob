require 'spec_helper'

describe SideJob::Port do
  before do
    @job = SideJob.queue('testq', 'TestWorker')
    @port = @job.input(:port1)
    @memory_job = SideJob.queue('testq', 'TestWorkerMemory')
    @memory = @memory_job.input(:memory)
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

  describe '#to_s' do
    it 'returns the redis key' do
      expect(SideJob::Port.new(@job, :in, :port1).to_s).to eq "job:#{@job.jid}:in:port1"
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

    it 'returns 1 if there is default value' do
      @port.default = [1,2]
      expect(@port.size).to eq 1
    end
  end

  describe '#data?' do
    it 'returns false when the port is empty' do
      expect(@port.data?).to be false
    end

    it 'returns true when the port has data' do
      @port.write 1
      expect(@port.data?).to be true
    end

    it 'works for memory ports' do
      expect(@memory.data?).to be false
      @memory.write 1
      expect(@memory.data?).to be true
    end

    it 'works when there is a default value on the port' do
      @port.default = [1,2]
      expect(@port.data?).to be true
    end
  end

  describe '#mode=' do
    it 'can change mode to memory' do
      expect(@port.mode).to eq :queue
      @port.mode = :memory
      expect(@port.mode).to eq :memory
      expect(SideJob.find(@job.jid).input(@port.name).mode).to eq :memory
    end

    it 'can change mode to queue' do
      expect(@memory.mode).to eq :memory
      @memory.mode = :queue
      expect(@memory.mode).to eq :queue
      expect(SideJob.find(@job.jid).input(@memory.name).mode).to eq :queue
    end

    it 'raises error changing mode to memory for output port' do
      expect { @job.output(:port).mode = :memory }.to raise_error
    end
  end

  describe '#default=' do
    it 'can change default value' do
      expect(@port.default).to be nil
      @port.default = [1,2]
      expect(@port.default).to eq [1,2]
      expect(SideJob.find(@job.jid).input(@port.name).default).to eq [1,2]
    end

    it 'raises error changing default value for output port' do
      expect { @job.output(:port).default = true }.to raise_error
    end
  end

  describe '#write' do
    it 'can write different types of data to a port' do
      @port.write('abc', 123, true, false, nil, {abc: 123}, [1, {foo: true}])
      data = SideJob.redis.lrange(@port.redis_key, 0, -1)
      expect(data).to eq(['"abc"', '123', 'true', 'false', 'null', '{"abc":123}', '[1,{"foo":true}]'])
    end

    it 'writing to a memory port should only store most recent value' do
      @memory.write(1, 2, 3)
      @memory.write('abc', 123, true, false, nil, {abc: 123}, [1, {foo: true}])
      data = SideJob.redis.lrange(@memory.redis_key, 0, -1)
      expect(data).to eq(['[1,{"foo":true}]'])
    end

    it 'logs writes' do
      now = Time.now
      Time.stub(:now).and_return(now)
      SideJob.redis.del "#{@job.redis_key}:log"
      @port.write('abc', 123)
      logs = SideJob.redis.lrange("#{@job.redis_key}:log", 0, -1).map {|log| JSON.parse(log)}
      expect(logs).to eq [{'type' => 'write', 'inport' => 'port1', 'data' => 123, 'timestamp' => SideJob.timestamp},
                          {'type' => 'write', 'inport' => 'port1', 'data' => 'abc', 'timestamp' => SideJob.timestamp},]
    end

    it 'logs writes by another job' do
      now = Time.now
      Time.stub(:now).and_return(now)
      SideJob.redis.del "#{@job.redis_key}:log"
      @job = SideJob.find(@job.jid, by: 'test:job')
      @port = @job.input(:port1)
      @port.write('abc')
      log = SideJob.redis.lpop("#{@job.redis_key}:log")
      expect(JSON.parse(log)).to eq({'type' => 'write', 'by' => 'test:job', 'inport' => 'port1', 'data' => 'abc', 'timestamp' => SideJob.timestamp})
    end

    it 'runs job when writing to an input port' do
      set_status(@job, 'suspended')
      inport = @job.input(:port1)
      inport.write('abc')
      expect(@job.status).to eq 'queued'
    end

    it 'runs parent job when writing to an output port' do
      child = SideJob.queue('testq', 'TestWorker', {parent: @job})
      set_status(@job, 'suspended')
      outport = child.output(:port1)
      outport.write('abc')
      expect(@job.status).to eq 'queued'
    end
  end

  describe '#read' do
    it 'can distinguish reading nil data and no data' do
      expect { @port.read }.to raise_error(EOFError)
      @port.write nil
      expect(@port.read).to be nil
    end

    it 'can read data from a queue port' do
      expect { @port.read }.to raise_error(EOFError)
      @port.write('abc', 123, true, false, nil, {}, ['data1', 1, {key: 'val'}])
      expect(@port.size).to be(7)
      expect(@port.read).to eq('abc')
      expect(@port.read).to eq(123)
      expect(@port.read).to eq(true)
      expect(@port.read).to eq(false)
      expect(@port.read).to eq(nil)
      expect(@port.read).to eq({})
      expect(@port.read).to eq(['data1', 1, {'key' => 'val'}])
      expect { @port.read }.to raise_error(EOFError)
      expect(@port.size).to be(0)
    end

    it 'can read data from a memory port' do
      expect { @memory.read }.to raise_error(EOFError)
      @memory.write 1, 2, 3, 4, 5
      expect(@memory.size).to be(1)
      3.times { expect(@memory.read).to eq(5) }
      expect(@memory.size).to be(1)
    end

    it 'can use default value' do
      @port.default = [1,2]
      3.times { expect(@port.read).to eq([1,2]) }
      @port.write true
      expect(@port.read).to be true
      3.times { expect(@port.read).to eq([1,2]) }
    end

    it 'logs reads' do
      now = Time.now
      Time.stub(:now).and_return(now)
      @port.write('abc', 123)
      SideJob.redis.del "#{@job.redis_key}:log"
      expect(@port.read).to eq('abc')
      expect(@port.read).to eq(123)
      logs = SideJob.redis.lrange("#{@job.redis_key}:log", 0, -1).
          map {|log| JSON.parse(log)}
      expect(logs).to eq [{'type' => 'read', 'inport' => 'port1', 'data' => 123, 'timestamp' => SideJob.timestamp},
                          {'type' => 'read', 'inport' => 'port1', 'data' => 'abc', 'timestamp' => SideJob.timestamp},]
    end

    it 'logs reads by another job' do
      now = Time.now
      Time.stub(:now).and_return(now)
      @port.write('abc')
      SideJob.redis.del "#{@job.redis_key}:log"
      @job = SideJob.find(@job.jid, by: 'test:job')
      @port = @job.input(:port1)
      expect(@port.read).to eq('abc')
      log = SideJob.redis.lpop("#{@job.redis_key}:log")
      expect(JSON.parse(log)).to eq({'type' => 'read', 'by' => 'test:job', 'inport' => 'port1', 'data' => 'abc', 'timestamp' => SideJob.timestamp})
    end
  end

  describe 'is Enumerable' do
    it 'can iterate over port elements' do
      10.times {|i| @port.write i}
      num = 0
      @port.each_with_index do |elem, i|
        expect(elem).to eq i
        num += 1
      end
      expect(num).to eq 10
    end

    it '#entries returns all data as an array' do
      expect(@port.entries).to eq []
      10.times {|i| @port.write i}
      expect(@port.entries).to eq Array(0..9)
      expect(@port.entries).to eq []
    end

    it 'iterates over a memory port by returning a single element' do
      expect(@memory.entries).to eq []
      10.times {|i| @memory.write i}
      expect(@memory.entries).to eq [9]
      expect(@memory.entries).to eq [9]
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
end
