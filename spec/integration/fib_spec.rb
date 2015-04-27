require 'spec_helper'

# fibonacci calculation by recursively spawning jobs
class TestFib
  include SideJob::Worker
  register(
      inports: {
          n: {}
      },
      outports: {
          num: {}
      }
  )
  def perform
    if input(:n).data?
      n = input(:n).read
    else
      n = get(:n)
    end
    suspend unless n
    set({n: n})

    if n <= 2
      output(:num).write 1
    else
      job1 = child(:job1)
      if ! job1
        job1 = queue('testq', 'TestFib', name: :job1)
        job1.input(:n).write n-1
      end

      job2 = child(:job2)
      if ! job2
        job2 = queue('testq', 'TestFib', name: :job2)
        job2.input(:n).write n-2
      end

      if job1.status != 'completed' || job2.status != 'completed'
        suspend
      else
        output(:num).write (job1.output(:num).read + job2.output(:num).read)
      end
    end
  end
end

describe TestFib do
  it 'calculates fibonacci correctly' do
    job = SideJob.queue('testq', 'TestFib')
    job.input(:n).write 6 # 1, 1, 2, 3, 5, 8
    SideJob::Worker.drain_queue
    expect(job.status).to eq 'completed'
    expect(job.output(:num).read).to eq(8)
  end
end
