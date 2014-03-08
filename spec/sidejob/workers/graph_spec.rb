require 'spec_helper'

describe SideJob::Graph do
  it 'sum three numbers' do
    graph = "
[Sum1] = test TestSum
[Sum2] = test TestSum
[Wait] = test TestWait total:1

@:start -> ready:[Sum1]
@:x -> in:[Sum1]
@:y -> in:[Sum1]
[Sum1]:sum -> in:[Sum2]
@:z -> in:[Sum2]

[Sum2]:sum-> out:@
[Sum1]:sum -> in:[Wait]
[Wait]:ready -> ready:[Sum2]
"
    job = SideJob.queue('testq', 'SideJob::Graph', {graph: graph})
    job.input(:x).push 3
    job.input(:y).push 4
    job.input(:z).push 5
    job.input(:start).push 1
    Timeout::timeout(5) { Sidekiq::Worker.drain_all }
    expect(job.status).to be(:completed)
    expect(job.output(:out).pop).to eq('12')
  end
end
