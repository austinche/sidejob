require 'spec_helper'

class TestSumFlow
  include SideJob::Worker
  register(
      inports: {},
      outports: {
          out: {},
      }
  )
  def perform
    sum = child(:sum)
    if ! sum
      queue('testq', 'TestSum', name: :sum)
      suspend
    else
      if get(:sent)
        output(:out).write sum.output(:sum).read
      else
        sum.input(:in).write 5
        sum.input(:in).write 6
        sum.input(:ready).write 1
        set(sent: 1)
        sum.run
        suspend
      end
    end
  end
end

describe TestSumFlow do
  it 'calls child job to sum numbers' do
    job = SideJob.queue('testq', 'TestSumFlow')
    SideJob::Worker.drain_queue
    expect(job.status).to eq 'completed'
    expect(job.output(:out).read).to eq(11)
  end
end
