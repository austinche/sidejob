require 'spec_helper'

describe SideJob::GraphParser do
  it 'can parse a graph' do
    job = SideJob::GraphParser.new
    job.input(:in).push(%Q{
      # parse this
      [Job1] = testq Foo::Bar # comment
      [Job2] = testq Foo::Baz
      [Job1]:out -> in:[Job2]:out -> in2:[Job1]:out2 -> in3:[Job1] + in4:[Job1]
      [Job1]:out2 -> in2:[Job2] + x:[Job1]
    })
    job.perform
    expect(job.output(:out).pop_json).to eq({'jobs' => {
        'Job1' => {'queue' => 'testq', 'class' => 'Foo::Bar', 'connections' => {'out' => [{'job' => 'Job2', 'port' => 'in'}], 'out2' => [{'job' => 'Job1', 'port' => 'in3'}, {'job' => 'Job1', 'port' => 'in4'}, {'job' => 'Job2', 'port' => 'in2'}, {'job' => 'Job1', 'port' => 'x'}]}},
        'Job2' => {'queue' => 'testq', 'class' => 'Foo::Baz', 'connections' => {'out' => [{'job' => 'Job1', 'port' => 'in2'}]}},
    }})
  end
end
