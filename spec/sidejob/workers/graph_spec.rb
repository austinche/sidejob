require 'spec_helper'

describe SideJob::Graph do
  it 'sum three numbers' do
    graph = TestSum3Worker.new.graph
    job = SideJob.queue('testq', 'SideJob::Graph', [graph])
    job.input('X').push 3
    job.input('Y').push 4
    job.input('Z').push 5
    job.input('START').push 1
    Timeout::timeout(5) { Sidekiq::Worker.drain_all }
    expect(job.status).to be(:completed)
    expect(job.output('OUT').pop).to eq('12')
  end
end
