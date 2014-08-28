require 'spec_helper'

describe SideJob::Job do
  describe '#==, #eql?' do
    it 'two jobs with the same jid are eq' do
      expect(SideJob::Job.new('123')).to eq(SideJob::Job.new('123'))
      expect(SideJob::Job.new('123')).to eql(SideJob::Job.new('123'))
    end

    it 'two jobs with different jid are not eq' do
      expect(SideJob::Job.new('123')).not_to eq(SideJob::Job.new('456'))
      expect(SideJob::Job.new('123')).not_to eql(SideJob::Job.new('456'))
    end
  end

  describe '#hash' do
    it 'uses hash of the job id and can be used as hash keys' do
      job = SideJob::Job.new('abc')
      expect(job.hash).to eq('abc'.hash)
      h = {}
      h[job] = 1
      job2 = SideJob::Job.new('abc')
      expect(job.hash).to eq(job2.hash)
      h[job2] = 3
      expect(h.keys.length).to be(1)
      expect(h[job]).to be(3)
    end
  end

  describe '#exists?' do
    it 'returns true if job exists' do
      @job = SideJob.queue('testq', 'TestWorker')
      expect(@job.exists?).to be true
    end
    it 'returns false if job does not exist' do
      expect(SideJob::Job.new('job').exists?).to be false
    end
  end

  describe '#info' do
    it 'returns all job info' do
      @job = SideJob.queue('testq', 'TestWorker', {args: [1, 2]})
      expect(@job.info).to eq({queue: 'testq', class: 'TestWorker', args: [1, 2], parent: nil, restart: nil, status: :queued })
    end
  end

  describe '#args=' do
    it 'sets job arguments and restarts the job' do
      @job = SideJob.queue('testq', 'TestWorker', {args: [1, 2]})
      @job.status = :completed
      expect(@job.info[:args]).to eq([1,2])
      @job.args = [3]
      expect(@job.status).to be :queued
      expect(@job.info[:args]).to eq([3])
    end
  end

  describe '#log_push, #log_pop' do
    it 'adds a timestamp to log entries' do
      now = Time.now
      Time.stub(:now).and_return(now)
      job = SideJob.queue('testq', 'TestWorker')
      while job.log_pop; end
      job.log_push('foo', {abc: 123})
      expect(job.log_pop).to eq({'type' => 'foo', 'abc' => 123, 'timestamp' => now.to_s})
    end
  end

  describe '#status, #status=' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'store status as symbol and loads as symbol' do
      @job.status = :newstatus
      expect(SideJob::Job.new(@job.jid).status).to eq(:newstatus)
    end

    it 'store status as string and loads as symbol' do
      @job.status = 'newstatus'
      expect(SideJob::Job.new(@job.jid).status).to eq(:newstatus)
    end

    it 'logs status changes' do
      now = Time.now
      Time.stub(:now).and_return(now)
      while @job.log_pop; end
      @job.status = 'newstatus'
      expect(@job.log_pop).to eq({'type' => 'status', 'status' => 'newstatus', 'timestamp' => now.to_s})
    end
  end

  describe '#children, #parent' do
    it 'can get children and parent jobs' do
      parent = SideJob.queue('testq', 'TestWorker')
      child = SideJob.queue('q2', 'TestWorker', {parent: parent})
      expect(TestWorker.jobs.size).to eq(2)
      expect(parent.children).to eq([child])
      expect(child.parent).to eq(parent)
    end
  end

  describe '#tree' do
    it 'recursively gets job tree' do
      job1 = SideJob.queue('q', 'TestWorker')
      job2 = SideJob.queue('q', 'TestWorker', {parent: job1})
      job3 = SideJob.queue('q', 'TestWorker', {parent: job1})
      job4 = SideJob.queue('q', 'TestWorker', {parent: job2})
      job5 = SideJob.queue('q', 'TestWorker', {parent: job4})
      expect(job1.tree).to match_array([{job: job2, children: [{job: job4, children: [{job: job5, children: []}]}]}, {job: job3, children: []}])
    end
  end

  describe '#restart' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'does nothing on a queued job' do
      expect(@job.status).to eq(:queued)
      expect { @job.restart }.to change(TestWorker.jobs, :size).by(0)
      expect(@job.status).to eq(:queued)
    end

    it 'restarts a completed job' do
      @job.status = :completed
      expect { @job.restart }.to change(TestWorker.jobs, :size).by(1)
      expect(@job.status).to eq(:queued)
    end

    it 'restarts a suspended job' do
      @job.status = :suspended
      expect { @job.restart }.to change(TestWorker.jobs, :size).by(1)
      expect(@job.status).to eq(:queued)
    end

    it 'schedules a job to run' do
      @job.status = :completed
      time = Time.now.to_f + 10000
      expect { @job.restart(time) }.to change(TestWorker.jobs, :size).by(1)
      expect(TestWorker.jobs.last['at']).to eq(time)
      expect(@job.status).to eq(:scheduled)
    end

    it 'schedules a job to run with a Time object' do
      @job.status = :completed
      time = Time.now + 10000
      expect { @job.restart(time) }.to change(TestWorker.jobs, :size).by(1)
      expect(TestWorker.jobs.last['at']).to eq(time.to_f)
      expect(@job.status).to eq(:scheduled)
    end

    it 'sets future immediate restart for running queued job' do
      @job.status = :running
      expect { @job.restart }.to change(TestWorker.jobs, :size).by(0)
      expect(@job.status).to eq(:running)
      expect(SideJob.redis {|redis| redis.hget @job.redis_key, :restart}).to eq('0')
    end

    it 'sets future scheduled restart for running queued job' do
      @job.status = :running
      expect { @job.restart(123) }.to change(TestWorker.jobs, :size).by(0)
      expect(@job.status).to eq(:running)
      expect(SideJob.redis {|redis| redis.hget @job.redis_key, :restart}).to eq('123')
    end

    it 'does nothing if job is already scheduled for sooner than requested' do
      Sidekiq::Testing.disable!
      stats = Sidekiq::Stats.new
      @job.status = :completed
      time = Time.now.to_f + 10000
      expect { @job.restart(time) }.to change(stats, :scheduled_size).by(1)
      job = Sidekiq::ScheduledSet.new.find_job(@job.jid)
      expect(job.at).to eq(Time.at(time))
      expect(@job.status).to eq(:scheduled)
      expect { @job.restart(time+1000) }.to change(stats, :scheduled_size).by(0)
      job = Sidekiq::ScheduledSet.new.find_job(@job.jid)
      expect(job.at).to eq(Time.at(time))
      expect(@job.status).to eq(:scheduled)
    end

    it 'deletes old scheduled job if it was scheduled for later than requested' do
      Sidekiq::Testing.disable!
      stats = Sidekiq::Stats.new
      @job.status = :completed
      time = Time.now.to_f + 10000
      expect { @job.restart(time) }.to change(stats, :scheduled_size).by(1)
      job = Sidekiq::ScheduledSet.new.find_job(@job.jid)
      expect(job.at).to eq(Time.at(time))
      expect(@job.status).to eq(:scheduled)
      expect { @job.restart(time-1000) }.to change(stats, :scheduled_size).by(0)
      job = Sidekiq::ScheduledSet.new.find_job(@job.jid)
      expect(job.at).to eq(Time.at(time-1000))
      expect(@job.status).to eq(:scheduled)
    end

    it 'immediately queues an already scheduled job' do
      Sidekiq::Testing.disable!
      time = Time.now.to_f + 10000
      @job.status = :completed
      @job.restart(time)
      expect(@job.status).to eq(:scheduled)
      expect(Sidekiq::ScheduledSet.new.find_job(@job.jid)).not_to be nil
      @job.restart
      expect(@job.status).to eq(:queued)
      expect(Sidekiq::ScheduledSet.new.find_job(@job.jid)).to be nil
    end
  end

  describe '#restart_in' do
    it 'calls #restart with the time' do
      @job = SideJob.queue('testq', 'TestWorker')
      now = Time.now
      Time.stub(:now).and_return(now)
      time = now.to_f + 1000
      expect(@job).to receive(:restart).with(time)
      @job.restart_in(1000)
    end
  end

  describe '#restarting?' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
      @job.status = :running
    end

    it 'returns false if not restarting' do
      expect(@job.restarting?).to be false
    end

    it 'returns true if restart called' do
      @job.restart
      expect(@job.restarting?).to be true
    end
  end

  describe '#delete' do
    before do
      @job = SideJob.queue('testq', 'TestWorker')
    end

    it 'recursively deletes jobs' do
      child = SideJob.queue('q2', 'TestWorker', {parent: @job})
      expect(@job.status).to eq(:queued)
      expect(child.status).to eq(:queued)
      expect(SideJob.redis {|redis| redis.keys('job:*').length}).to be > 0
      @job.delete
      expect(SideJob.redis {|redis| redis.keys('job:*').length}).to be(0)
      expect(@job.status).to be_nil
      expect(child.status).to be_nil
    end

    it 'deletes data on input and output ports' do
      @job.input('port1').write 'data'
      @job.output('port2').write 'data'
      expect(@job.inports).to eq([@job.input('port1')])
      expect(@job.outports).to eq([@job.output('port2')])
      @job.delete
      expect(@job.inports).to eq([])
      expect(@job.outports).to eq([])
      expect(@job.input('port1').read).to be_nil
      expect(@job.output('port2').read).to be_nil
      expect(SideJob.redis {|redis| redis.keys('job:*').length}).to be(0)
    end
  end

  describe '#input' do
    it 'returns an input port' do
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.input('port')).to eq(SideJob::Port.new(job, :in, 'port'))
    end
  end

  describe '#output' do
    it 'returns an output port' do
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.output('port')).to eq(SideJob::Port.new(job, :out, 'port'))
    end
  end

  describe '#inports' do
    it 'returns input ports that have data' do
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.inports.size).to be(0)
      job.input('port1').write 'abc'
      expect(job.inports.size).to be(1)
      expect(job.inports[0].name).to eq 'port1'
      job.input('port2').read
      expect(job.inports.size).to be(1)
      job.input('port2').write 'abc'
      expect(job.inports.size).to be(2)
      job.input('port2').read
      expect(job.inports.size).to be(1)
    end
  end

  describe '#outports' do
    it 'returns output ports that have data' do
      job = SideJob.queue('testq', 'TestWorker')
      expect(job.outports.size).to be(0)
      job.output('port1').write 'abc'
      expect(job.outports.size).to be(1)
      expect(job.outports[0].name).to eq 'port1'
      job.output('port2').read
      expect(job.outports.size).to be(1)
      job.output('port2').write 'abc'
      expect(job.outports.size).to be(2)
      job.output('port2').read
      expect(job.outports.size).to be(1)
    end
  end
end
