require 'spec_helper'

describe SideJob::DelayedCopy do
  it 'copies data from input to output with default no delay' do
    job = SideJob::DelayedCopy.new
    job.input('IN').push 'a'
    job.input('IN').push 'b'
    Timeout::timeout(0.5) { job.perform }
    expect(job.output('OUT').pop).to eq('a')
    expect(job.output('OUT').pop).to eq('b')
  end

  it 'can add delay' do
    job = SideJob::DelayedCopy.new
    job.input('DELAY').push 1
    job.input('IN').push 'a'
    job.input('IN').push 'b'
    expect { Timeout::timeout(0.5) { job.perform } }.to raise_error(TimeoutError)
  end
end
