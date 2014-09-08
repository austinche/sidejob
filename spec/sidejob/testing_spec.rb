require 'spec_helper'

describe 'SideJob testing helpers' do
  class TestLongRunning
    include SideJob::Worker
    def perform
      sleep 3
    end
  end

  class TestFailure
    include SideJob::Worker
    def perform
      raise 'bad error'
    end
  end

  describe 'SideJob::Worker.drain_queue' do
    it 'runs jobs' do
      job = SideJob.queue('myq', 'TestSum')
      5.times {|i| job.input(:in).write i}
      job.input(:ready).write 1
      job2 = SideJob.queue('myq', 'TestSum')
      6.times {|i| job2.input(:in).write i}
      job2.input(:ready).write 1
      expect(job.output(:sum).read).to be nil
      expect(job2.output(:sum).read).to be nil
      SideJob::Worker.drain_queue
      expect(job.output(:sum).read).to eq '10'
      expect(job2.output(:sum).read).to eq '15'
    end

    it 'can specify a timeout' do
      job = SideJob.queue('test', 'TestLongRunning')
      expect { SideJob::Worker.drain_queue(timeout: 0.25) }.to raise_error(Timeout::Error)
    end

    it 'raises errors by default' do
      job = SideJob.queue('test', 'TestFailure')
      expect { SideJob::Worker.drain_queue }.to raise_error(RuntimeError, 'bad error')
    end

    it 'can disable raising of errors' do
      job = SideJob.queue('test', 'TestFailure')
      expect { SideJob::Worker.drain_queue(raise_on_errors: false) }.not_to raise_error
    end
  end
end