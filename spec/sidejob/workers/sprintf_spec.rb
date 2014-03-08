require 'spec_helper'

describe SideJob::Sprintf do
  it 'interpolates strings' do
    job = SideJob::Sprintf.new
    job.input(:word1).push 'hello'
    job.input(:word2).push 'world'
    job.perform({'format' => '%{word1} %{word2}'})
    expect(job.output(:out).pop).to eq('hello world')
    job.input(:word1).push 'bye'
    job.perform({'format' => '%{word1} %{word2}'})
    expect(job.output(:out).pop).to eq('bye world')
  end
end
