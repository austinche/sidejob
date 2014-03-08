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

  describe '#push, #pop, #size' do
    it 'can push and pop data to a port' do
      expect(@port.pop).to be_nil
      expect(@port.size).to be(0)
      @port.push('abc', 123, JSON.generate(['data1', 1, {key: 'val'}]))
      expect(@port.size).to be(3)
      expect(@port.pop).to eq('abc')
      expect(@port.pop).to eq('123')
      expect(JSON.parse(@port.pop)).to eq(['data1', 1, {'key' => 'val'}])
      expect(@port.pop).to be_nil
      expect(@port.size).to be(0)
    end
  end

  describe '#pop_all_to' do
    it 'pops all data from one port and pushes it to another' do
      dst = SideJob::Port.new(@job, :out, 'port2')
      @port.push '1'
      @port.push '2'
      @port.push '3'
      expect(@port.size).to be(3)
      expect(dst.size).to be(0)
      expect(@port.pop_all_to(dst)).to eq(['1', '2', '3'])
      expect(dst.size).to be(3)
      expect(dst.pop).to eq('1')
      expect(dst.pop).to eq('2')
      expect(dst.pop).to eq('3')
      expect(dst.pop).to be_nil
    end
  end

  describe '#peek' do
    it 'returns data without changing port' do
      @port.push('abc', 123)
      expect(@port.size).to be(2)
      expect(@port.peek).to eq('abc')
      expect(@port.size).to be(2)
      expect(@port.peek).to eq('abc')
    end

    it 'returns nil when port is empty' do
      expect(@port.size).to be(0)
      expect(@port.peek).to be_nil
    end
  end

  describe '#trim' do
    it 'does nothing if given size is bigger than data on port' do
      @port.push('abc', 'def', 'ghi')
      expect(@port.size).to be(3)
      @port.trim(3)
      expect(@port.size).to be(3)
    end

    it 'removes the oldest data items' do
      @port.push('abc', 'def', 'ghi')
      expect(@port.size).to be(3)
      @port.trim(2)
      expect(@port.size).to be(2)
      expect(@port.peek).to eq('def')
    end
  end

  describe '#clear' do
    it 'empties all data on the port' do
      @port.push('abc', 'def', 'ghi')
      expect(@port.size).to be(3)
      @port.clear
      expect(@port.size).to be(0)
    end
  end

  describe '#redis_key' do
    it 'returns key with valid name' do
      expect(@port.redis_key).to eq('job:in:port1')
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

    it 'returns ports that have had remember called' do
      expect(SideJob::Port.all(@job, :in)).to eq([])
      @port.remember
      expect(SideJob::Port.all(@job, :in)).to match_array([@port])
    end
  end

  describe '.delete_all' do
    it 'delete all port keys' do
      expect(SideJob.redis {|conn| conn.keys('*').length}).to be(0)
      @port.push 'abc'
      keys = SideJob.redis {|conn| conn.keys('*').length}
      SideJob::Port.new(@job, :out, 'port2').push 'abc'
      SideJob::Port.new(@job, :out, 'port3').push 'abc'
      SideJob::Port.delete_all(@job, :out)
      expect(SideJob.redis {|conn| conn.keys('*').length}).to be(keys)
      SideJob::Port.delete_all(@job, :in)
      expect(SideJob.redis {|conn| conn.keys('*').length}).to be(0)
    end
  end
end
