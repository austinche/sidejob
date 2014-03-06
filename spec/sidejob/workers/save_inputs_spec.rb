require 'spec_helper'

describe SideJob::SaveInputs do
  it 'sends nothing to output if not triggered' do
    job = SideJob::SaveInputs.new
    job.input('PORT1').push '1'
    job.input('PORT1').push '2'
    job.input('PORT2').push '3'
    job.perform
    expect(job.output('PORT1').pop).to be_nil
  end

  it 'copies data to output when triggered' do
    job = SideJob::SaveInputs.new
    job.input('PORT1').push '1'
    job.input('PORT1').push '2'
    job.input('PORT2').push '3'
    job.input('TRIGGER').push ''
    job.perform
    expect(job.output('PORT1').pop).to eq('1')
    expect(job.output('PORT1').pop).to eq('2')
    expect(job.output('PORT2').pop).to eq('3')
    expect(job.output('PORT1').pop).to be_nil
    expect(job.output('PORT2').pop).to be_nil
    job.input('TRIGGER').push ''
    job.perform
    expect(job.output('PORT1').pop).to eq('1')
    expect(job.output('PORT1').pop).to eq('2')
    expect(job.output('PORT2').pop).to eq('3')
  end
end
