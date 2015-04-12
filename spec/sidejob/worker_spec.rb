require 'spec_helper'

describe SideJob::Worker do
  before do
    @job = SideJob.queue('testq', 'TestWorker', inports: {
        in1: {},
        in2: {},
        memory: { mode: :memory },
        default: { default: 'default' },
        default_null: { default: nil },
    }, outports: {out1: {}})
    @worker = TestWorker.new
    @worker.jid = @job.id
    @worker.status = 'running'
  end

  describe '.register_all' do
    it 'overwrites existing data with current registry' do
      spec = {abc: [1, 2]}
      SideJob.redis.hmset 'workers:q1', 'foo', 'bar'
      SideJob::Worker.register_all('q1')
      expect(SideJob.redis.hget('workers:q1', 'foo')).to be nil
    end
  end

  describe '.config' do
    it 'returns nil for a non-existing worker' do
      expect(SideJob::Worker.config('noq', 'NoWorker')).to be nil
    end

    it 'returns a worker config that has been registered for the current queue' do
      expect(SideJob::Worker.config('testq', 'TestWorker')).to eq JSON.parse(SideJob::Worker.registry['TestWorker'].to_json)
    end

    it 'returns a worker config that has been registered elsewhere' do
      config = {'abc' => [1, 2]}
      SideJob.redis.hmset 'workers:q1', 'NewWorker', config.to_json
      expect(SideJob::Worker.config('q1', 'NewWorker')).to eq config
    end
  end

  describe '.register' do
    class TestWorkerRegister
      include SideJob::Worker
      register(
          my_register_key: [1, 2, 3]
      )
      def perform
      end
    end

    it 'registers a worker configuration' do
      expect(SideJob::Worker.registry['TestWorkerRegister']).to eq({my_register_key: [1,2,3]})
    end
  end

  it 'includes SideJob::JobMethods' do
    expect(TestWorker.included_modules).to include(SideJob::JobMethods)
  end

  it '#suspend raises exception' do
    expect { @worker.suspend }.to raise_error(SideJob::Worker::Suspended)
  end

  describe '#for_inputs' do
    it 'does nothing if no ports provided' do
      expect {|block| @worker.for_inputs(&block)}.not_to yield_control
    end

    it 'yields data from input ports' do
      @job.input(:in1).write 1
      @job.input(:in1).write 'a'
      @job.input(:in2).write [2, 3]
      @job.input(:in2).write foo: 123
      expect {|block| @worker.for_inputs(:in1, :in2, &block)}.to yield_successive_args([1, [2,3]], ['a', {'foo' => 123}])
    end

    it 'logs input and output from them' do
      now = Time.now
      allow(Time).to receive(:now) { now }
      @job.input(:in1).write 1
      @job.input(:in1).write 2
      @job.input(:in2).write ['a', 'b']
      @job.input(:in2).write ['c', 'd']
      SideJob.logs(clear: true)
      @worker.for_inputs(:in1, :in2) do |in1, in2|
        @worker.output(:out1).write [in1, in2[0]]
      end
      expect(SideJob.logs).to eq([{'timestamp' => SideJob.timestamp, 'read' => [{'job' => @job.id, 'inport' => 'in1', 'data' => [1]}, {'job' => @job.id, 'inport' => 'in2', 'data' => [['a', 'b']]}], 'write' => [{'job' => @job.id, 'outport' => 'out1', 'data' => [[1, 'a']]}]},
                                  {'timestamp' => SideJob.timestamp, 'read' => [{'job' => @job.id, 'inport' => 'in1', 'data' => [2]}, {'job' => @job.id, 'inport' => 'in2', 'data' => [['c', 'd']]}], 'write' => [{'job' => @job.id, 'outport' => 'out1', 'data' => [[2, 'c']]}]},
                                 ])
    end

    it 'suspends on partial inputs' do
      @job.input(:in1).write 1
      @job.input(:in2).write [2, 3]
      @job.input(:in2).write 3
      expect {
        expect {|block| @worker.for_inputs(:in1, :in2, &block)}.to yield_successive_args([1, [2,3]])
      }.to raise_error(SideJob::Worker::Suspended)
    end

    it 'returns data from memory input ports' do
      @job.input(:memory).write 1
      @job.input(:in2).write [2, 3]
      @job.input(:in2).write 3
      expect {|block| @worker.for_inputs(:memory, :in2, &block)}.to yield_successive_args([1, [2,3]], [1, 3])
    end

    it 'does not suspend if there is only data on memory port' do
      @job.input(:memory).write 1
      expect {|block| @worker.for_inputs(:memory, :in2, &block)}.not_to yield_control
    end

    it 'allows for null default values' do
      @job.input(:default_null).write 1
      @job.input(:in2).write [2, 3]
      @job.input(:in2).write 3
      expect {|block| @worker.for_inputs(:default_null, :in2, &block)}.to yield_successive_args([1, [2,3]], [nil, 3])
    end

    it 'raises error if all ports have defaults' do
      @job.input(:memory).write true
      expect {|block| @worker.for_inputs(:memory, :default, &block)}.to raise_error
    end
  end
end
