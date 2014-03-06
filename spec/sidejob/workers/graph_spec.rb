require 'spec_helper'

describe SideJob::Graph do
  it 'raises error if component does not specify queue' do
    graph = "'abc' -> IN Test(TestWorker)"
    if false
    expect {
      job = SideJob::Graph.new
      job.jid = 'jid'
      job.perform(graph)
    }.to raise_error(RuntimeError)
    end
    graph = "'abc' -> IN Test(testq/TestWorker)"
    expect {
      job = SideJob::Graph.new
      job.jid = 'jid'
      job.perform(graph)
    }.to raise_error(SideJob::Worker::Suspended)
  end

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

  describe '#fbp_compile' do
    it 'compiles fbp language to Hash' do
      expect(SideJob::Graph.new.fbp_compile("")).to eq({"processes"=>{}, "connections"=>[]})
    end
  end
end
