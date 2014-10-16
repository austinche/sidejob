require 'spec_helper'

describe 'SideJob testing helpers' do
  class TestLongRunning
    include SideJob::Worker
    register
    def perform
      sleep 3
    end
  end

  class TestFailure
    include SideJob::Worker
    register
    def perform
      raise 'bad error'
    end
  end

  class TestMysteriousFailure
    include SideJob::Worker
    register
    def perform
      set status: 'failed'
    end
  end

  describe 'SideJob::Worker.drain_queue' do
    it 'runs jobs' do
      job = SideJob.queue('testq', 'TestSum')
      5.times {|i| job.input(:in).write i}
      job.input(:ready).write 1
      job2 = SideJob.queue('testq', 'TestSum')
      6.times {|i| job2.input(:in).write i}
      job2.input(:ready).write 1
      expect(job.output(:sum).data?).to be false
      expect(job2.output(:sum).data?).to be false
      SideJob::Worker.drain_queue
      expect(job.output(:sum).read).to eq 10
      expect(job2.output(:sum).read).to eq 15
    end

    it 'can specify a timeout' do
      job = SideJob.queue('testq', 'TestLongRunning')
      expect { SideJob::Worker.drain_queue(timeout: 0.25) }.to raise_error(Timeout::Error)
    end

    it 'raises errors by default' do
      job = SideJob.queue('testq', 'TestFailure')
      expect { SideJob::Worker.drain_queue }.to raise_error(RuntimeError, 'bad error')
    end

    it 'raises error if worker mysteriously fails' do
      job = SideJob.queue('testq', 'TestMysteriousFailure')
      expect { SideJob::Worker.drain_queue }.to raise_error(RuntimeError)
    end

    it 'can disable raising of errors' do
      job = SideJob.queue('testq', 'TestFailure')
      expect { SideJob::Worker.drain_queue(errors: false) }.not_to raise_error
    end
  end

  describe 'SideJob::Job#run_inline' do
    it 'runs a single job once' do
      job = SideJob.queue('testq', 'TestSum')
      5.times {|i| job.input(:in).write i}
      job.input(:ready).write 1
      expect(job.output(:sum).data?).to be false
      job.run_inline
      expect(job.status).to eq 'completed'
      expect(job.output(:sum).read).to eq 10
    end

    it 'queues a non-queued job by default' do
      job = SideJob.queue('testq', 'TestSum')
      5.times {|i| job.input(:in).write i}
      job.input(:ready).write 1
      job.set status: :suspended
      job.run_inline
      expect(job.output(:sum).read).to eq 10
    end

    it 'can turn off queuing of a job' do
      job = SideJob.queue('testq', 'TestSum')
      5.times {|i| job.input(:in).write i}
      job.input(:ready).write 1
      job.set status: :suspended
      job.run_inline queue: false
      expect(job.output(:sum).data?).to be false
    end

    it 'raises errors by default' do
      job = SideJob.queue('testq', 'TestFailure')
      expect { job.run_inline }.to raise_error(RuntimeError, 'bad error')
    end

    it 'raises error if worker mysteriously fails' do
      job = SideJob.queue('testq', 'TestMysteriousFailure')
      expect { job.run_inline }.to raise_error(RuntimeError)
    end

    it 'can disable raising of errors' do
      job = SideJob.queue('testq', 'TestFailure')
      expect { job.run_inline(errors: false) }.not_to raise_error
    end
  end
end
