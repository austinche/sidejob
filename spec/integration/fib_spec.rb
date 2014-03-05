require 'spec_helper'

# fibonnaci calculation by recursively spawning jobs
class TestFib
  include SideJob::Worker
  def perform(*args)
    n = get(:n)
    if ! n
      n = input('N').pop
      suspend unless n
      set(:n, n)
    end

    n = n.to_i
    if n <= 2
      output('NUM').push '1'
    else
      jobs = mget(:job1, :job2)
      if ! jobs[:job1]
        jobs[:job1] = queue('testq', 'TestFib')
        jobs[:job1].input('N').push n-1
        set(:job1, jobs[:job1].jid)
      end

      if ! jobs[:job2]
        jobs[:job2] = queue('testq', 'TestFib')
        jobs[:job2].input('N').push n-2
        set(:job2, jobs[:job2].jid)
      end

      suspend if children.length != 2 || children[0].status != :completed || children[1].status != :completed

      output('NUM').push (children[0].output('NUM').pop.to_i + children[1].output('NUM').pop.to_i)
    end
  end
end

describe TestFib do
  it 'calculates fibonnaci correctly' do
    job = SideJob.queue('testq', 'TestFib')
    job.input('N').push 6 # 1, 1, 2, 3, 5, 8
    Sidekiq::Worker.drain_all

    expect(job.status).to be(:completed)
    expect(job.output('NUM').pop).to eq('8')
  end
end
