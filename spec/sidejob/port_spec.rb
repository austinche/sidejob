require 'spec_helper'

describe SideJob::Port do
  before do
    @job = SideJob.queue('testq', 'TestWorker', inports: {
        port1: {},
        memory: { mode: :memory },
        default: { default: 'default' },
        default_null: { default: nil },
        default_false: { default: false},
        memory_with_default: { mode: :memory, default: 'memory default' },
    }, outports: {
        out1: {},
    })
    @port1 = @job.input(:port1)
    @out1 = @job.output(:out1)
    @memory = @job.input(:memory)
    @default = @job.input(:default)
    @defaults = [
        @default,
        @job.input(:default_null),
        @job.input(:default_false),
        @job.input(:memory_with_default)
    ]
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
      expect(SideJob::Port.new(@job, :in, :port1)).to eq(@port1)
      expect(SideJob::Port.new(@job, :in, 'port1')).to eq(@port1)
      expect(SideJob::Port.new(@job, :in, 'port1')).to eql(@port1)
    end

    it 'two ports with different names are not eq' do
      expect(SideJob::Port.new(@job, :in, 'port2')).not_to eq(@port1)
      expect(SideJob::Port.new(@job, :in, 'port2')).not_to eql(@port1)
    end

    it 'port names are case sensitive' do
      expect(SideJob::Port.new(@job, :in, 'PORT1')).not_to eq(@port1)
      expect(SideJob::Port.new(@job, :in, 'PORT1')).not_to eql(@port1)
    end
  end

  describe '#to_s' do
    it 'returns the redis key' do
      expect(SideJob::Port.new(@job, :in, :port1).to_s).to eq "job:#{@job.id}:in:port1"
    end
  end

  describe '#exists?' do
    it 'returns true if port exists' do
      expect(@port1.exists?).to be true
    end
    
    it 'returns false if port does not exist' do
      expect(SideJob::Port.new(@job, :in, 'missing').exists?).to be false
    end
  end

  describe '#options=' do
    it 'can change mode to memory' do
      expect(@port1.mode).to eq :queue
      @port1.options = { mode: :memory }
      expect(@port1.mode).to eq :memory
    end

    it 'can change mode to queue' do
      expect(@memory.mode).to eq :memory
      @memory.options = { mode: :queue }
      expect(@memory.mode).to eq :queue
    end

    it 'can set default value' do
      expect(@port1.default?).to be false
      @port1.options = { default: [1,2] }
      expect(@port1.default).to eq [1,2]
      expect(@port1.read).to eq [1,2]
    end

    it 'can remove default value' do
      expect(@default.default?).to be true
      @default.options = { }
      expect(@default.default?).to be false
    end
  end

  describe '#mode' do
    it 'returns nil for non-existent ports' do
      expect(SideJob::Port.new(@job, :in, 'missing').mode).to be nil
    end

    it 'returns the port mode for queue ports' do
      expect(@port1.mode).to eq :queue
      expect(@default.mode).to eq :queue
    end

    it 'returns the port mode for memory ports' do
      expect(@memory.mode).to eq :memory
      expect(@job.input(:memory_with_default).mode).to eq :memory
    end
  end

  describe '#size' do
    it 'returns 0 when the port is empty' do
      expect(@port1.size).to eq 0
    end

    it 'number of elements is increased on write' do
      expect {
        @port1.write 1
      }.to change(@port1, :size).by(1)
    end

    it 'number of elements is decreased on read' do
      @port1.write 1
      expect {
        @port1.read
      }.to change(@port1, :size).by(-1)
    end

    it 'returns 0 if there is default value' do
      @defaults.each {|port| expect(port.size).to eq 0 }
    end
  end

  describe '#data?' do
    it 'returns false when the port is empty' do
      expect(@port1.data?).to be false
    end

    it 'returns true when the port has data' do
      @port1.write 1
      expect(@port1.data?).to be true
    end

    it 'works for memory ports' do
      expect(@memory.data?).to be false
      @memory.write 1
      expect(@memory.data?).to be true
    end

    it 'works when there is a default value on the port' do
      @defaults.each {|port| expect(port.data?).to be true }
    end
  end

  describe '#default' do
    it 'returns nil for no default' do
      expect(@port1.default).to be nil
    end

    it 'returns parsed object by default' do
      @port1.options = {default: [1,2]}
      expect(@port1.default).to eq [1,2]
    end

    it 'can return default as json' do
      @port1.options = {default: [1,2]}
      expect(@port1.default(json: true)).to eq '[1,2]'
    end

    it 'can return null default value' do
      @port1.options = {default: nil}
      expect(@port1.default(json: true)).to eq 'null'
      expect(@port1.default).to be nil
      expect(@port1.default?).to be true
    end
  end

  describe '#default?' do
    it 'returns false for normal port' do
      expect(@port1.default?).to be false
    end

    it 'returns false for memory port with no data written' do
      expect(@memory.default?).to be false
    end

    it 'returns true for memory port after writing data' do
      @memory.write 1
      expect(@memory.default?).to be true
    end

    it 'returns true for port with default value' do
      @defaults.each {|port| expect(port.default?).to be true }
    end
  end

  describe '#write' do
    it 'can write different types of data to a port' do
      ['abc', 123, true, false, nil, {abc: 123}, [1, {foo: true}]].each {|x| @port1.write x}
      data = SideJob.redis.lrange(@port1.redis_key, 0, -1)
      expect(data).to eq(['"abc"', '123', 'true', 'false', 'null', '{"abc":123}', '[1,{"foo":true}]'])
    end

    it 'writing to a memory port should only set default value to latest value' do
      @memory.write 'abc'
      @memory.write [1, {foo: true}]
      @memory.write 'latest'
      expect(@memory.size).to eq 0
      expect(@memory.default).to eq 'latest'
    end

    it 'logs writes' do
      now = Time.now
      allow(Time).to receive(:now) { now }
      @job.group_port_logs do
        @port1.write 'abc'
        @port1.write 123
      end
      expect(SideJob.logs).to eq [{'timestamp' => SideJob.timestamp, 'read' => [],
                                   'write' => ['job' => @job.id, 'inport' => 'port1', 'data' => ['abc', 123]]}]
    end

    it 'raises error if port does not exist' do
      expect { SideJob::Port.new(@job, :in, 'foo').write true }.to raise_error
    end

    it 'raises error if port mode is unknown ' do
      SideJob.redis.hset "#{@job.redis_key}:inports:mode", 'foo', '???'
      expect { SideJob::Port.new(@job, :in, 'foo').write true }.to raise_error
    end

    it 'runs the job if it is an input port' do
      expect(@port1.job).to receive(:run)
      @port1.write 3
    end

    it 'does not run the job if it is an output port' do
      expect(@out1.job).not_to receive(:run)
      @out1.write 3
    end

    it 'does not run the job if it is a memory port' do
      expect(@memory.job).not_to receive(:run)
      @memory.write 3
    end
  end

  describe '#read' do
    it 'can distinguish reading nil data and no data' do
      expect { @port1.read }.to raise_error(EOFError)
      @port1.write nil
      expect(@port1.read).to be nil
    end

    it 'can read data from a queue port' do
      expect { @port1.read }.to raise_error(EOFError)
      ['abc', 123, true, false, nil, {}, ['data1', 1, {key: 'val'}]].each {|x| @port1.write x}
      expect(@port1.size).to be(7)
      expect(@port1.read).to eq('abc')
      expect(@port1.read).to eq(123)
      expect(@port1.read).to eq(true)
      expect(@port1.read).to eq(false)
      expect(@port1.read).to eq(nil)
      expect(@port1.read).to eq({})
      expect(@port1.read).to eq(['data1', 1, {'key' => 'val'}])
      expect { @port1.read }.to raise_error(EOFError)
      expect(@port1.size).to be(0)
    end

    it 'can read data from a memory port' do
      expect { @memory.read }.to raise_error(EOFError)
      5.times {|i| @memory.write i }
      3.times { expect(@memory.read).to eq(4) }
    end

    it 'can use default value' do
      @defaults.each do |port|
        3.times { expect(port.read).to eq port.default }
        port.write 'mydata'
        expect(port.read).to eq 'mydata'
        3.times { expect(port.read).to eq port.mode == :memory ? 'mydata' : port.default }
      end
    end

    it 'can use null default value' do
      port = @job.input(:default_null)
      expect(port.read).to eq nil
    end

    it 'can use false default value' do
      port = @job.input(:default_false)
      expect(port.read).to be false
    end

    it 'logs reads' do
      now = Time.now
      allow(Time).to receive(:now) { now }
      ['abc', 123].each {|x| @port1.write x}
      SideJob.logs
      @job.group_port_logs do
        expect(@port1.read).to eq('abc')
        expect(@port1.read).to eq(123)
      end
      expect(SideJob.logs).to eq [{'timestamp' => SideJob.timestamp,
                                   'read' => ['job' => @job.id, 'inport' => 'port1', 'data' => ['abc', 123]],
                                   'write' => []}]
    end
  end

  describe '#connect_to' do
    it 'does nothing on an empty port' do
      expect(@out1.connect_to(@port1)).to eq []
      expect(@port1.data?).to be false
    end

    it 'sends data to a port' do
      @out1.write 1
      @out1.write [2,3]
      expect(@out1.connect_to(@port1)).to eq [1, [2,3]]
      expect(@out1.data?).to be false
      expect(@port1.read).to eq 1
      expect(@port1.read).to eq [2,3]
      expect(@port1.data?).to be false
    end

    it 'sends data to all destination ports' do
      dest = [@port1, @memory, @default]
      @out1.write 1
      @out1.write [2,3]
      @out1.connect_to dest
      expect(@port1.read).to eq 1
      expect(@port1.read).to eq [2,3]
      expect(@port1.data?).to be false
      expect(@memory.size).to eq 0
      expect(@memory.read).to eq [2,3]
      expect(@default.read).to eq 1
      expect(@default.read).to eq [2,3]
      expect(@default.read).to eq 'default'
    end

    it 'passes port default values to all destinations' do
      dest = [@port1, @memory, @out1]
      @default.write 1
      @default.write [2,3]
      @default.connect_to dest
      expect(@port1.read).to eq 1
      expect(@port1.read).to eq [2,3]
      expect(@port1.default).to eq 'default'
      expect(@memory.size).to eq 0
      expect(@memory.default).to eq 'default'
      expect(@out1.read).to eq 1
      expect(@out1.read).to eq [2,3]
      expect(@out1.default).to eq 'default'
    end

    it 'runs job for normal input port' do
      expect(@port1.job).to receive(:run)
      @out1.write true
      @out1.connect_to @port1
    end

    it 'does not run job if no data sent' do
      expect(@port1.job).not_to receive(:run)
      @out1.connect_to @port1
    end

    it 'does not run job for memory port' do
      expect(@memory.job).not_to receive(:run)
      @out1.write true
      @out1.connect_to @memory
    end

    it 'logs data' do
      now = Time.now
      allow(Time).to receive(:now) { now }
      @out1.write 1
      @out1.write [2,3]
      SideJob.logs(clear: true)
      @out1.connect_to(@port1)
      expect(SideJob.logs).to eq([{'timestamp' => SideJob.timestamp,
                                   'read' => [{'job' => @job.id, 'outport' => 'out1', 'data' => [1,[2,3]]}],
                                   'write' => [{'job' => @job.id, 'inport' => 'port1', 'data' => [1,[2,3]]}] }])
    end

    it 'can specify additional metadata to add to log' do
      now = Time.now
      allow(Time).to receive(:now) { now }
      job2 = SideJob.queue('testq', 'TestWorker')
      @out1.write 1
      @out1.write [2,3]
      SideJob.logs(clear: true)
      @out1.connect_to(@port1, user: 'test')
      expect(SideJob.logs).to eq([{'timestamp' => SideJob.timestamp, 'user' => 'test',
                                   'read' => [{'job' => @job.id, 'outport' => 'out1', 'data' => [1,[2,3]]}],
                                   'write' => [{'job' => @job.id, 'inport' => 'port1', 'data' => [1,[2,3]]}] }])
    end

    it 'does not log if no data on port' do
      job2 = SideJob.queue('testq', 'TestWorker')
      @out1.connect_to(@port1)
      expect(SideJob.logs).to eq([])
    end
  end

  describe 'is Enumerable' do
    it 'can iterate over port elements' do
      10.times {|i| @port1.write i}
      num = 0
      @port1.each_with_index do |elem, i|
        expect(elem).to eq i
        num += 1
      end
      expect(num).to eq 10
    end

    it '#entries returns all data as an array' do
      expect(@port1.entries).to eq []
      10.times {|i| @port1.write i}
      expect(@port1.entries).to eq Array(0..9)
      expect(@port1.entries).to eq []
    end

    it 'default values are not returned' do
      @defaults.each do |port|
        expect(port.entries).to eq []
        port.write 'mydata'
        if port.mode == :memory
          expect(port.entries).to eq []
        else
          expect(port.entries).to eq ['mydata']
        end
      end
    end
  end

  describe '#redis_key' do
    it 'returns key with valid name' do
      expect(@port1.redis_key).to eq("#{@port1.job.redis_key}:in:port1")
    end
  end

  describe '#hash' do
    it 'uses hash of the redis key' do
      expect(@port1.hash).to eq(@port1.redis_key.hash)
    end

    it 'can be used as keys in a hash' do
      h = {}
      h[@port1] = 1
      port2 = SideJob::Port.new(@job, :in, 'port1')
      expect(@port1.hash).to eq(port2.hash)
      h[port2] = 3
      expect(h.keys.length).to be(1)
      expect(h[@port1]).to be(3)
    end
  end
end
