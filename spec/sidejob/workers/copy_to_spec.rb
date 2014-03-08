require 'spec_helper'

describe SideJob::CopyTo do
  it 'copies input data to a different port name' do
    job = SideJob::CopyTo.new
    job.input(:in).push 'hello'
    job.input(:in).push 'world'
    job.perform({'port' => 'MYPORT'})
    expect(job.output('MYPORT').pop).to eq('hello')
    expect(job.output('MYPORT').pop).to eq('world')
  end
end
