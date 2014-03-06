require 'spec_helper'

describe SideJob::Count do
  it 'counts packets received' do
    job = SideJob::Count.new
    job.input('IN').push '1'
    job.input('IN').push '1'
    job.perform
    expect(job.output('COUNT').pop).to eq('2')
    job.input('IN').push '1'
    job.perform
    expect(job.output('COUNT').pop).to eq('3')
  end

  it 'can reset count' do
    job = SideJob::Count.new
    job.input('IN').push '1'
    job.input('IN').push '1'
    job.perform
    expect(job.output('COUNT').pop).to eq('2')
    job.input('RESET').push '1'
    job.perform
    expect(job.output('COUNT').pop).to eq('0')
  end

  it 'can record progress' do
    job = SideJob::Count.new
    expect(job).to receive(:notify).twice
    job.input('TOTAL').push '9'
    job.input('IN').push '1'
    job.input('IN').push '1'
    job.perform
  end
end
