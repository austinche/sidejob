require 'spec_helper'

describe SideJob::CopyTo do
  it 'copies input data to a different port name' do
    job = SideJob::CopyTo.new
    job.input('PORT').push 'MYPORT'
    job.input('IN').push 'hello'
    job.input('IN').push 'world'
    job.perform
    expect(job.output('MYPORT').pop).to eq('hello')
    expect(job.output('MYPORT').pop).to eq('world')
  end
end
