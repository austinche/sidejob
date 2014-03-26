require 'spec_helper'

describe SideJob::Filter do
  it 'pass through filter' do
    job = SideJob::Filter.new
    data = [{'abc' => 123, 'xyz' => 'foo'}, [1, 2, 3, "abc"]]
    data.each do |x|
      job.input(:in).push_json x
    end
    job.perform({'filter' => '.'})
    data.each do |x|
      expect(job.output(:out).pop_json).to eq x
    end
  end

  it 'lookup filter number' do
    job = SideJob::Filter.new
    job.input(:in).push_json({"foo" => 42, "bar" => "hello"})
    job.perform({'filter' => '.foo'})
    expect(job.output(:out).pop).to eq '42'
  end

  it 'lookup filter string' do
    job = SideJob::Filter.new
    job.input(:in).push_json({"foo" => 42, "bar" => "hello"})
    job.perform({'filter' => '.bar'})
    expect(job.output(:out).pop).to eq '"hello"'
  end

  it 'string interpolation' do
    job = SideJob::Filter.new
    job.input(:in).push_json({"foo" => 42, "bar" => "hello"})
    job.perform({'filter' => '"\(.bar) world: \(.foo+1)"'})
    expect(job.output(:out).pop).to eq '"hello world: 43"'
  end

  it 'length calculation' do
    job = SideJob::Filter.new
    job.input(:in).push_json({"foo" => [1, 2, 3]})
    job.perform({'filter' => '.foo | length'})
    expect(job.output(:out).pop).to eq '3'
  end

end
