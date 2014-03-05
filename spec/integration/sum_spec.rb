require 'spec_helper'

class TestSumFlow
  include SideJob::Worker
  def perform(*args)
    if children.length == 0
      queue('testq', 'TestSum')
      suspend
    else
      if ! get(:sent)
        children[0].input('IN').push '5'
        children[0].input('IN').push '6'
        children[0].input('READY').push '1'
        set(:sent, 1)
        children[0].restart
        suspend
      end

      sum = children[0].output('SUM').pop
      suspend unless sum
      output('OUT').push sum
    end
  end
end

describe TestSumFlow do
  it 'calls child job to sum numbers' do
    job = SideJob.queue('testq', 'TestSumFlow')
    Sidekiq::Worker.drain_all
    expect(job.status).to be(:completed)
    expect(job.output('OUT').pop).to eq('11')
  end
end
