require 'spec_helper'

describe SideJob::Graph do
  it 'sum three numbers' do
    graph = {
      inports: {
        start: {
          process: 'Sum1',
          port: 'ready',
        },
        x: {
          process: 'Sum1',
          port: 'in',
        },
        y: {
          process: 'Sum1',
          port: 'in',
        },
        z: {
          process: 'Sum2',
          port: 'in',
        },
      },
      outports: {
        out: {
          process: 'Sum2',
          port: 'sum',
        },
      },
      processes: {
        Sum1: { component: 'test/TestSum' },
        Sum2: { component: 'test/TestSum' },
        Wait: { component: 'test/TestWait' },
      },
      connections: [
        {
          data: '1',
          tgt: {
            process: 'Wait',
            port: 'total',
          },
        },
        {
          src: {
            process: 'Sum1',
            port: 'sum',
          },
          tgt: {
            process: 'Sum2',
            port: 'in',
          },
        },
        {
          src: {
            process: 'Sum1',
            port: 'sum',
          },
          tgt: {
            process: 'Wait',
            port: 'in',
          },
        },
        {
          src: {
            process: 'Wait',
            port: 'ready',
          },
          tgt: {
            process: 'Sum2',
            port: 'ready',
          },
        },
      ],
    }

    job = SideJob.queue('testq', 'SideJob::Graph')
    job.input(:graph).push_json graph
    job.input(:x).push 3
    job.input(:y).push 4
    job.input(:z).push 5
    job.input(:start).push 1
    Timeout::timeout(5) { Sidekiq::Worker.drain_all }
    expect(job.status).to be(:completed)
    expect(job.output(:out).pop).to eq('12')
  end

  it 'sends data to outport correctly if another job also uses the output' do
    graph = {
      inports: {
        start: {
          process: 'Sum1',
          port: 'ready',
        },
        x: {
          process: 'Sum1',
          port: 'in',
        },
        y: {
          process: 'Sum1',
          port: 'in',
        },
      },
      outports: {
        result: {
          process: 'Sum1',
          port: 'sum',
        },
      },
      processes: {
        Sum1: { component: 'test/TestSum' },
        Dummy: { component: 'test/TestWait' },
      },
      connections: [
        {
          data: '0',
          tgt: {
            process: 'Dummy',
            port: 'total',
          },
        },
        {
          src: {
            process: 'Sum1',
            port: 'sum',
          },
          tgt: {
            process: 'Dummy',
            port: 'ignore',
          },
        },
      ],
    }

    job = SideJob.queue('testq', 'SideJob::Graph')
    job.input(:graph).push_json graph
    job.input(:x).push 3
    job.input(:y).push 4
    job.input(:start).push 1
    Timeout::timeout(5) { Sidekiq::Worker.drain_all }
    expect(job.status).to be(:completed)
    expect(job.output(:result).pop).to eq('7')
  end
end
