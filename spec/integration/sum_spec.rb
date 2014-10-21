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
    if children.length == 0
      queue('testq', 'TestSum')
      suspend
    else
      if get(:sent)
        output(:out).write children[0].output(:sum).read
      else
        children[0].input(:in).write 5
        children[0].input(:in).write 6
        children[0].input(:ready).write 1
        set(sent: 1)
        children[0].run
        suspend
      end
    end
  end
end

describe TestSumFlow do
  it 'calls child job to sum numbers' do
    job = SideJob.queue('testq', 'TestSumFlow')
    SideJob::Worker.drain_queue
    job.reload
    expect(job.status).to eq 'completed'
    expect(job.output(:out).read).to eq(11)
  end
end
