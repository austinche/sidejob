require 'spec_helper'

describe SideJob::Sprintf do
  it 'interpolates strings' do
    job = SideJob::Sprintf.new
    job.input('FORMAT').push '%{word1} %{word2}'
    job.input('word1').push 'hello'
    job.input('word2').push 'world'
    job.perform
    expect(job.output('OUT').pop).to eq('hello world')
    job.input('word1').push 'bye'
    job.perform
    expect(job.output('OUT').pop).to eq('bye world')
  end
end
