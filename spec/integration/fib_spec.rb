require 'spec_helper'

# fibonnaci calculation by recursively spawning jobs
class TestFib
  include SideJob::Worker
  def perform(*args)
    n = get(:n)
    if ! n
      n = input('N').read
      suspend unless n
      set(:n, n)
    end

    n = n.to_i
    if n <= 2
      output('NUM').write '1'
    else
      jobs = mget(:job1, :job2)
      if ! jobs[:job1]
        jobs[:job1] = queue('testq', 'TestFib')
        jobs[:job1].input('N').write n-1
        set(:job1, jobs[:job1].jid)
      end

      if ! jobs[:job2]
        jobs[:job2] = queue('testq', 'TestFib')
        jobs[:job2].input('N').write n-2
        set(:job2, jobs[:job2].jid)
      end

      if children.length != 2 || children[0].status != 'completed' || children[1].status != 'completed'
        suspend
      else
        output('NUM').write (children[0].output('NUM').read.to_i + children[1].output('NUM').read.to_i)
      end
    end
  end
end

describe TestFib do
  it 'calculates fibonnaci correctly' do
    job = SideJob.queue('testq', 'TestFib')
    job.input('N').write 6 # 1, 1, 2, 3, 5, 8
    SideJob::Worker.drain_queue

    expect(job.status).to eq 'completed'
    expect(job.output('NUM').read).to eq('8')
  end
end
