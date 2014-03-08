require 'spec_helper'

describe SideJob::DelayedCopy do
  it 'copies data from input to output with default no delay' do
    job = SideJob::DelayedCopy.new
    job.input(:in).push 'a'
    job.input(:in).push 'b'
    Timeout::timeout(0.5) { job.perform }
    expect(job.output(:out).pop).to eq('a')
    expect(job.output(:out).pop).to eq('b')
  end

  it 'can add delay' do
    job = SideJob::DelayedCopy.new
    job.input(:in).push 'a'
    job.input(:in).push 'b'
    expect { Timeout::timeout(0.5) { job.perform({'delay' => 1}) } }.to raise_error(TimeoutError)
  end
end
