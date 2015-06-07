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
      expect(@port1.default).to be nil
      expect(@port1.default?).to be true
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
      ['abc', 123, true, false, nil, {abc: 123}, [1, {foo: true}]].each {|x| @port1.write x}
      data = SideJob.redis.lrange(@port1.redis_key, 0, -1)
      expect(data).to eq(['"abc"', '123', 'true', 'false', 'null', '{"abc":123}', '[1,{"foo":true}]'])
    end

    it 'logs writes' do
      now = Time.now
      allow(Time).to receive(:now) { now }
      expect(SideJob).to receive(:publish).with('/sidejob/log', {read: [], write: [{job: @port1.job.id, inport: :port1, data: ['abc', 123]}], timestamp: SideJob.timestamp})
      SideJob::Port.log_group do
        @port1.write 'abc'
        @port1.write 123
      end
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
      expect(port.read).to be false
    end

    it 'logs reads' do
      now = Time.now
      allow(Time).to receive(:now) { now }
      ['abc', 123].each {|x| @port1.write x}
      expect(SideJob).to receive(:publish).with('/sidejob/log', {read: [{job: @port1.job.id, inport: :port1, data: ['abc', 123]}], write: [], timestamp: SideJob.timestamp})
      SideJob::Port.log_group do
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
      now = Time.now
      allow(Time).to receive(:now) { now }
      @out1.write 1
      @out1.write [2,3]
      expect(SideJob).to receive(:publish).with('/sidejob/log', {read: [{job: @out1.job.id, outport: :out1, data: [1,[2,3]]}], write: [{job: @out1.job.id, inport: :port1, data: [1,[2,3]]}], timestamp: SideJob.timestamp})
      @out1.connect_to(@port1)
    end

    it 'can use SideJob.context to add context to log entry' do
      now = Time.now
      allow(Time).to receive(:now) { now }
      @out1.write 1
      @out1.write [2,3]
      expect(SideJob).to receive(:publish).with('/sidejob/log', {user: 'test', read: [{job: @out1.job.id, outport: :out1, data: [1,[2,3]]}], write: [{job: @out1.job.id, inport: :port1, data: [1,[2,3]]}], timestamp: SideJob.timestamp})
      SideJob.context(user: 'test') do
        @out1.connect_to(@port1)
      end
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

      expect(SideJob).to receive(:publish).with('channel1', 1)
      expect(SideJob).to receive(:publish).with('channel1', [2,3])
      expect(SideJob).to receive(:publish)
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

  describe '.log_group' do
    before do
      now = Time.now
      allow(Time).to receive(:now) { now }
    end

    it 'does not log anything if no port operations occur within the block' do
      expect(SideJob).not_to receive(:publish)
      SideJob::Port.log_group {}
    end

    it 'groups all port logs within the block' do
      expect(SideJob).to receive(:publish).with('/sidejob/log', {
          read: [{job: @port1.job.id, inport: :port1, data: ['abc']}],
          write: [{job: @port1.job.id, inport: :port1, data: ['abc', 'def']},
                  {job: @out1.job.id, outport: :out1, data: ['xyz']}],
          timestamp: SideJob.timestamp})
      SideJob::Port.log_group do
        @port1.write 'abc'
        @port1.read
        @port1.write 'def'
        @out1.write 'xyz'
      end
    end

    it 'does not write out log until the end of outermost group' do
      expect(SideJob).to receive(:publish).with('/sidejob/log', {
          read: [],
          write: [{job: @port1.job.id, inport: :port1, data: ['hello', 2]}],
          timestamp: SideJob.timestamp})
      SideJob::Port.log_group do
        @port1.write 'hello'
        SideJob::Port.log_group do
          @port1.write 2
        end
      end
    end

    it 'works with SideJob.context' do
      expect(SideJob).to receive(:publish).with('/sidejob/log', {
          user: 'foo',
          read: [{job: @port1.job.id, inport: :port1, data: ['abc']}],
          write: [{job: @port1.job.id, inport: :port1, data: ['abc']}],
          timestamp: SideJob.timestamp})
      SideJob.context(user: 'foo') do
        SideJob::Port.log_group do
          @port1.write 'abc'
          @port1.read
        end
      end
    end

    it 'logs correctly even if data is changed' do
      expect(SideJob).to receive(:publish).with('/sidejob/log', {
          read: [{job: @port1.job.id, inport: :port1, data: [{'x' => [1,2]},{'x' => [1,2,3]}]}],
          write: [{job: @port1.job.id, inport: :port1, data: [{'x' => [1,2]},{'x' => [1,2,3]}]}],
          timestamp: SideJob.timestamp})
      data = {'x' => [1,2]}
      SideJob::Port.log_group do
        @port1.write data
        expect(@port1.read).to eq data
        data['x'].push 3
        @port1.write data
        expect(@port1.read).to eq data
      end
    end
  end
end
