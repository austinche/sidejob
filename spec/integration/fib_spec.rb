require 'spec_helper'

# fibonacci calculation by recursively spawning jobs
class TestFib
  include SideJob::Worker
  register(
      inports: {
          n: {mode: :memory}
      }
  )
  def perform
    suspend unless input(:n).data?
    n = input(:n).read

    if n <= 2
      output(:num).write 1
    else
      job1 = get(:job1)
      if ! job1
        job1 = queue('testq', 'TestFib')
        job1.input(:n).write n-1
        set(job1: job1.jid)
      end

      job2 = get(:job2)
      if ! job2
        job2 = queue('testq', 'TestFib')
        job2.input(:n).write n-2
        set(job2: job2.jid)
      end

      if children.length != 2 || children[0].status != 'completed' || children[1].status != 'completed'
        suspend
      else
        output(:num).write (children[0].output(:num).read + children[1].output(:num).read)
      end
    end
  end
end

describe TestFib do
  it 'calculates fibonnaci correctly' do
    job = SideJob.queue('testq', 'TestFib')
    job.input(:n).write 6 # 1, 1, 2, 3, 5, 8
    SideJob::Worker.drain_queue
    job.reload!
    expect(job.status).to eq 'completed'
    expect(job.output(:num).read).to eq(8)
  end
end
