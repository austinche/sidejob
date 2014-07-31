require 'spec_helper'

describe SideJob::Filter do
  it 'pass through filter' do
    job = SideJob::Filter.new
    data = [{'abc' => 123, 'xyz' => 'foo'}, [1, 2, 3, "abc"]]
    data.each do |x|
      job.input(:in).push_json x
    end
    job.input(:filter).push '.'
    job.perform
    data.each do |x|
      expect(job.output(:out).pop_json).to eq x
    end
  end

  it 'lookup filter number' do
    job = SideJob::Filter.new
    job.input(:in).push_json({"foo" => 42, "bar" => "hello"})
    job.input(:filter).push '.foo'
    job.perform
    expect(job.output(:out).pop).to eq '42'
  end

  it 'lookup filter string' do
    job = SideJob::Filter.new
    job.input(:in).push_json({"foo" => 42, "bar" => "hello"})
    job.input(:filter).push '.bar'
    job.perform
    expect(job.output(:out).pop).to eq '"hello"'
  end

  it 'string interpolation' do
    job = SideJob::Filter.new
    job.input(:in).push_json({"foo" => 42, "bar" => "hello"})
    job.input(:filter).push '"\(.bar) world: \(.foo+1)"'
    job.perform
    expect(job.output(:out).pop).to eq '"hello world: 43"'
  end

  it 'length calculation' do
    job = SideJob::Filter.new
    job.input(:in).push_json({"foo" => [1, 2, 3]})
    job.input(:filter).push '.foo | length'
    job.perform
    expect(job.output(:out).pop).to eq '3'
  end

end
