require 'spec_helper'

describe SideJob::Count do
  it 'counts packets received' do
    job = SideJob::Count.new
    job.input(:in).push '1'
    job.input(:in).push '1'
    job.perform
    expect(job.output(:count).pop).to eq('2')
    job.input(:in).push '1'
    job.perform
    expect(job.output(:count).pop).to eq('3')
  end

  it 'can reset count' do
    job = SideJob::Count.new
    job.input(:in).push '1'
    job.input(:in).push '1'
    job.perform
    expect(job.output(:count).pop).to eq('2')
    job.input(:reset).push '1'
    job.perform
    expect(job.output(:count).pop).to eq('0')
  end

  it 'can record progress' do
    job = SideJob::Count.new
    expect(job).to receive(:notify).at_least(1)
    job.input(:total).push '9'
    job.input(:in).push '1'
    job.input(:in).push '1'
    job.perform
  end
end
