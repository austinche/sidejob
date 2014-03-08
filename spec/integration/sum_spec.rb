require 'spec_helper'

class TestSumFlow
  include SideJob::Worker
  def perform(options)
    if children.length == 0
      queue('testq', 'TestSum')
      suspend
    else
      if ! get(:sent)
        children[0].input(:in).push '5'
        children[0].input(:in).push '6'
        children[0].input(:ready).push '1'
        set(:sent, 1)
        children[0].restart
        suspend
      end

      sum = children[0].output(:sum).pop
      suspend unless sum
      output(:out).push sum
    end
  end
end

describe TestSumFlow do
  it 'calls child job to sum numbers' do
    job = SideJob.queue('testq', 'TestSumFlow')
    Sidekiq::Worker.drain_all
    expect(job.status).to be(:completed)
    expect(job.output(:out).pop).to eq('11')
  end
end
