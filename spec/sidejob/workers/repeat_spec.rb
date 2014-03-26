require 'spec_helper'

describe SideJob::Repeat do
  it 'sends nothing to output if not triggered' do
    job = SideJob::Repeat.new
    job.input(:in).push '1'
    job.input(:in).push '2'
    job.input(:in).push '3'
    job.perform
    expect(job.output(:out).pop).to be_nil
  end

  it 'sends data to output when triggered' do
    job = SideJob::Repeat.new
    job.input(:in).push '1'
    job.input(:in).push '2'
    job.input(:in).push '3'
    job.input(:trigger).push ''
    job.perform
    expect(job.output(:out).pop).to eq('1')
    expect(job.output(:out).pop).to eq('2')
    expect(job.output(:out).pop).to eq('3')
    expect(job.output(:out).pop).to be_nil
    job.input(:trigger).push ''
    job.perform
    expect(job.output(:out).pop).to eq('1')
    expect(job.output(:out).pop).to eq('2')
    expect(job.output(:out).pop).to eq('3')
  end
end
