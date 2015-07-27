require 'spec_helper'

describe SideJob::Port do
  before do
    @job = SideJob.queue('testq', 'TestWorker', inports: {
        port1: {},
        default: { default: 'default' },
        default_null: { default: nil },
        default_false: { default: false},
    }, outports: {
        out1: {},
    })
    @port1 = @job.input(:port1)
    @out1 = @job.output(:out1)
    @default = @job.input(:default)
    @defaults = [
        @default,
        @job.input(:default_null),
        @job.input(:default_false),
    ]
  end

  describe '#initialize' do
    it 'raises error if name is invalid' do
      expect { SideJob::Port.new(@job, :in, 'PORT.1')}.to raise_error
    end

    it 'raises error if name is empty' do
      expect { SideJob::Port.new(@job, :in, '')}.to raise_error
    end

    it 'raises error on non-existent port' do
      expect { SideJob::Port.new(@job, :in, 'missing')}.to raise_error
    end

    it 'can dynamically create a port' do
      @job.inports = {
          '*' => { default: 123 },
      }
      expect(@job.input('abc').default).to eq 123
    end
  end

  describe '#==, #eql?' do
    before do
      @job.inports = { '*' => {}}
    end

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

    it 'returns true when there is a default value on the port' do
      @defaults.each {|port| expect(port.data?).to be true }
    end
  end

  describe '#default' do
    it 'returns default value' do
      @port1.default = [1,2]
      expect(@port1.default).to eq [1,2]
    end

    it 'returns None for no default' do
      expect(@port1.default).to be SideJob::Port::None
    end

    it 'can return null default value' do
      @port1.default = nil
      expect(@port1.default).to eq nil
      expect(@port1.default?).to eq true
    end
  end

  describe '#default?' do
    it 'returns false for normal port' do
      expect(@port1.default?).to be false
    end

    it 'returns true for port with default value' do
      @defaults.each {|port| expect(port.default?).to be true }
    end
  end

  describe '#default=' do
    it 'can set and overwrite the default' do
      [true, false, nil, 123, 'abc', {'xyz' => [1,2]}, [5,6]].each do |val|
        @port1.default = val
        expect(@port1.default).to eq(val)
      end
    end

    it 'can clear the default' do
      @port1.default = 1234
      expect(@port1.default?).to be true
      @port1.default = SideJob::Port::None
      expect(@port1.default?).to be false
    end
  end

  describe '#channels, #channels=' do
    before do
      @channels = ['abc123', 'def456']
    end

    it 'can set and return channels for inport' do
      expect(@port1.channels).to eq []
      @channels.each do |ch|
        expect(SideJob.redis.smembers("channel:#{ch}")).to eq []
      end
      @port1.channels = @channels
      expect(@port1.channels).to match_array(@channels)
      @channels.each do |ch|
        expect(SideJob.redis.smembers("channel:#{ch}")).to eq [@port1.job.id.to_s]
      end
      @port1.channels = []
      expect(@port1.channels).to eq []
      @channels.each do |ch|
        # We don't remove old jobs until we publish to the channel
        expect(SideJob.redis.smembers("channel:#{ch}")).to eq [@port1.job.id.to_s]
      end
    end

    it 'can set and return channels for outport' do
      expect(@out1.channels).to eq []
      @out1.channels = @channels
      @channels.each do |ch|
        expect(SideJob.redis.smembers("channel:#{ch}")).to eq []
      end
      expect(@out1.channels).to match_array(@channels)
      @channels.each do |ch|
        expect(SideJob.redis.smembers("channel:#{ch}")).to eq []
      end
      @out1.channels = []
      expect(@out1.channels).to eq []
    end
  end

  describe '#write' do
    it 'can write different types of data to a port' do
      data = ['abc', 123, true, false, nil, {'abc' => 123}, [1, {'foo' => true}]]
      data.each {|x| @port1.write x}
      expect(@port1.entries).to eq(data)
    end

    it 'logs writes' do
      expect(SideJob).to receive(:log).with({read: [], write: [{job: @port1.job.id, inport: :port1, data: ['abc', 123]}]})
      SideJob::Port.group do
        @port1.write 'abc'
        @port1.write 123
      end
    end

    it 'can disable logging for both reading and writing' do
      expect(SideJob).not_to receive(:log)
      SideJob::Port.group(log: false) do
        @port1.write 'abc'
      end
      expect(@port1.read).to eq 'abc'
    end

    it 'raises error if port does not exist' do
      expect { SideJob::Port.new(@job, :in, 'foo').write true }.to raise_error
    end

    it 'runs the job if it is an input port' do
      parent = SideJob.queue('testq', 'TestWorker')
      parent.adopt(@job, 'child')
      parent.status = 'completed'
      @job.status = 'completed'
      @port1.write 3
      expect(@job.status).to eq 'queued'
      expect(parent.status).to eq 'completed'
    end

    it 'runs the parent job if it is an output port' do
      parent = SideJob.queue('testq', 'TestWorker')
      parent.adopt(@job, 'child')
      parent.status = 'completed'
      @job.status = 'completed'
      @out1.write 3
      expect(@job.status).to eq 'completed'
      expect(parent.status).to eq 'queued'
    end

    it 'can disable running job' do
      parent = SideJob.queue('testq', 'TestWorker')
      parent.adopt(@job, 'child')
      parent.status = 'completed'
      @job.status = 'completed'
      SideJob::Port.group(notify: false) do
        @port1.write 3
        @out1.write 3
      end
      expect(@job.status).to eq 'completed'
      expect(parent.status).to eq 'completed'
    end

    it 'publishes writes to associated output port channel' do
      data = {'abc' => [1,2]}
      @out1.channels = ['mychannel']
      expect(SideJob).to receive(:publish).with('mychannel', data)
      expect(SideJob).to receive(:publish)
      @out1.write data
    end

    it 'does not publish writes to associated input port channel' do
      data = {'abc' => [1,2]}
      @port1.channels = ['mychannel']
      expect(SideJob).not_to receive(:publish).with('mychannel', data)
      @port1.write data
    end
  end

  describe '#read' do
    it 'can read different kinds of data' do
      expect(@port1.read).to eq SideJob::Port::None
      ['abc', 123, true, false, nil, {}, ['data1', 1, {key: 'val'}]].each {|x| @port1.write x}
      expect(@port1.size).to be(7)
      expect(@port1.read).to eq('abc')
      expect(@port1.read).to eq(123)
      expect(@port1.read).to eq(true)
      expect(@port1.read).to eq(false)
      expect(@port1.read).to eq(nil)
      expect(@port1.read).to eq({})
      expect(@port1.read).to eq(['data1', 1, {'key' => 'val'}])
      expect(@port1.read).to eq SideJob::Port::None
      expect(@port1.size).to be(0)
    end

    it 'can use default value' do
      @defaults.each do |port|
        3.times { expect(port.read).to eq port.default }
        port.write 'mydata'
        expect(port.read).to eq 'mydata'
        3.times { expect(port.read).to eq port.default }
      end
    end

    it 'can use null default value' do
      port = @job.input(:default_null)
      expect(port.read).to eq nil
    end

    it 'can use false default value' do
      port = @job.input(:default_false)
      expect(port.read).to eq false
    end

    it 'logs reads' do
      ['abc', 123].each {|x| @port1.write x}
      expect(SideJob).to receive(:log).with({read: [{job: @port1.job.id, inport: :port1, data: ['abc', 123]}], write: []})
      SideJob::Port.group do
        expect(@port1.read).to eq('abc')
        expect(@port1.read).to eq(123)
      end
    end

    it 'can disable read logging' do
      ['abc', 123].each {|x| @port1.write x}
      expect(SideJob).not_to receive(:log)
      SideJob::Port.group(log: false) do
        expect(@port1.read).to eq('abc')
        expect(@port1.read).to eq(123)
      end
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
      dest = [@port1, @default]
      @out1.write 1
      @out1.write [2,3]
      @out1.connect_to dest
      expect(@port1.read).to eq 1
      expect(@port1.read).to eq [2,3]
      expect(@port1.data?).to be false
      expect(@default.read).to eq 1
      expect(@default.read).to eq [2,3]
      expect(@default.read).to eq 'default'
    end

    it 'passes port default values to all destinations' do
      dest = [@port1, @out1]
      @default.write 1
      @default.write [2,3]
      @default.connect_to dest
      expect(@port1.read).to eq 1
      expect(@port1.read).to eq [2,3]
      expect(@port1.default).to eq 'default'
      expect(@out1.read).to eq 1
      expect(@out1.read).to eq [2,3]
      expect(@out1.default).to eq 'default'
    end

    it 'runs job if the default value has changed' do
      expect(@port1.job).to receive(:run)
      @default.connect_to @port1
    end

    it 'does not run job if the default value has not changed' do
      @default.connect_to @port1
      expect(@port1.job).not_to receive(:run)
      @default.connect_to @port1
    end

    it 'runs job for normal input port' do
      @out1.write true
      expect(@port1.job).to receive(:run)
      @out1.connect_to @port1
    end

    it 'runs parent job for outport' do
      parent = SideJob.queue('testq', 'TestWorker')
      parent.adopt(@job, 'child')
      parent.status = 'completed'
      @job.status = 'completed'
      j2 = SideJob.queue('testq', 'TestWorker', inports: {in: {}})
      j2.input(:in).write true
      j2.input(:in).connect_to @out1
      expect(@job.status).to eq 'completed'
      expect(parent.status).to eq 'queued'
    end

    it 'does not run job if no data sent' do
      expect(@port1.job).not_to receive(:run)
      @out1.connect_to @port1
    end

    it 'logs data' do
      @out1.write 1
      @out1.write [2,3]
      expect(SideJob).to receive(:log).with({read: [{job: @out1.job.id, outport: :out1, data: [1,[2,3]]}], write: [{job: @out1.job.id, inport: :port1, data: [1,[2,3]]}]})
      @out1.connect_to(@port1)
    end

    it 'does not log if no data on port' do
      expect(SideJob).not_to receive(:publish)
      @out1.connect_to(@port1)
    end

    it 'publishes to associated outport channels' do
      dest = [@port1, @out1]
      @out1.channels = ['channel1']
      @port1.channels = ['channel2']
      @default.channels = ['channel3']
      @default.write 1
      @default.write [2,3]

      allow(SideJob).to receive(:publish)
      expect(SideJob).to receive(:publish).with('channel1', 1)
      expect(SideJob).to receive(:publish).with('channel1', [2,3])
      @default.connect_to dest
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
        expect(port.entries).to eq ['mydata']
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

  describe '.group' do
    it 'does not log anything if no port operations occur within the block' do
      expect(SideJob).not_to receive(:log)
      SideJob::Port.group {}
    end

    it 'groups all port logs within the block' do
      expect(SideJob).to receive(:log).with({
          read: [{job: @port1.job.id, inport: :port1, data: ['abc']}],
          write: [{job: @port1.job.id, inport: :port1, data: ['abc', 'def']},
                  {job: @out1.job.id, outport: :out1, data: ['xyz']}],
      })
      SideJob::Port.group do
        @port1.write 'abc'
        @port1.read
        @port1.write 'def'
        @out1.write 'xyz'
      end
    end

    it 'does not write out log until the end of outermost group' do
      expect(SideJob).to receive(:log).with({
          read: [],
          write: [{job: @port1.job.id, inport: :port1, data: ['hello', 2]}],
      })
      SideJob::Port.group do
        @port1.write 'hello'
        SideJob::Port.group do
          @port1.write 2
        end
      end
    end

    it 'works with SideJob.context' do
      now = Time.now
      allow(Time).to receive(:now) { now }
      allow(SideJob).to receive(:publish)
      expect(SideJob).to receive(:publish).with('/sidejob/log', {
          user: 'foo',
          read: [{job: @port1.job.id, inport: :port1, data: ['abc']}],
          write: [{job: @port1.job.id, inport: :port1, data: ['abc']}],
          timestamp: SideJob.timestamp})
      SideJob.context(user: 'foo') do
        SideJob::Port.group do
          @port1.write 'abc'
          @port1.read
        end
      end
    end

    it 'logs correctly even if data is changed' do
      expect(SideJob).to receive(:log).with({
          read: [{job: @port1.job.id, inport: :port1, data: [{'x' => [1,2]},{'x' => [1,2,3]}]}],
          write: [{job: @port1.job.id, inport: :port1, data: [{'x' => [1,2]},{'x' => [1,2,3]}]}],
      })
      data = {'x' => [1,2]}
      SideJob::Port.group do
        @port1.write data
        expect(@port1.read).to eq data
        data['x'].push 3
        @port1.write data
        expect(@port1.read).to eq data
      end
    end

    it 'can set options' do
      SideJob::Port.group(log: true, notify: false, set_default: true) do
        expect(Thread.current[:sidejob_port_group][:options]).to eq({log: true, notify: false, set_default: true})
      end
      expect(Thread.current[:sidejob_port_group]).to be nil
    end

    it 'can merge options in nested groups' do
      SideJob::Port.group(log: false) do
        expect(Thread.current[:sidejob_port_group][:options]).to eq({log: false})
        SideJob::Port.group(notify: true) do
          expect(Thread.current[:sidejob_port_group][:options]).to eq({log: false, notify: true})
        end
        expect(Thread.current[:sidejob_port_group][:options]).to eq({log: false})
      end
      expect(Thread.current[:sidejob_port_group]).to be nil
    end
  end

  describe '.encode_data' do
    it 'encodes data with no context' do
      expect(JSON.parse(SideJob::Port.encode_data(5))).to eq({ 'data' => 5 })
    end

    it 'handles context' do
      SideJob.context({xyz: 456}) do
        expect(JSON.parse(SideJob::Port.encode_data([1,2]))).to eq({ 'context' => {'xyz' => 456}, 'data' => [1,2] })
      end
    end

    it 'handles port group options' do
      SideJob::Port.group(log: true, notify: false) do
        expect(JSON.parse(SideJob::Port.encode_data([1,2]))).to eq({ 'options' => {'log' => true, 'notify' => false}, 'data' => [1,2] })
      end
    end
  end

  describe '.decode_data' do
    it 'returns None if no data' do
      expect(SideJob::Port.decode_data(nil)).to be SideJob::Port::None
    end

    it 'decodes object with no context' do
      x = SideJob::Port.decode_data(SideJob::Port.encode_data(1.23))
      expect(x.sidejob_context).to eq({})
    end

    it 'handles context' do
      SideJob.context({abc: 'foo'}) do
        SideJob.context({xyz: 456}) do
          x = SideJob::Port.decode_data(SideJob::Port.encode_data(nil))
          expect(x.sidejob_context).to eq({'abc' => 'foo', 'xyz' => 456})
        end
      end
    end

    it 'handles port group options' do
      SideJob::Port.group(log: true, notify: false) do
        expect(SideJob::Port.decode_data(SideJob::Port.encode_data([1,2])).sidejob_options).to eq({ 'log' => true, 'notify' => false})
      end
    end

    it 'decoded data should equal original data' do
      ['abc', [1,2], {'abc' => [1,2]}, 1, 1.23, true, false, nil].each do |x|
        expect(SideJob::Port.decode_data(SideJob::Port.encode_data(x))).to eq x
      end
    end
  end
end
