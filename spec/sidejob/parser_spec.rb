require 'spec_helper'

require 'parslet/rig/rspec'
describe SideJob::Parser do
  context 'parse rules' do
    let(:parser) { SideJob::Parser.new }

    it 'parses comments' do
      expect(parser.comment).to parse('#')
      expect(parser.comment).to parse('# hello world')
    end

    it 'parses job definition' do
      expect(parser.job_definition).not_to parse("[job] = q")
      expect(parser.job_definition.parse("[job] = q class1")).to eq({job: 'job', queue: 'q', class: 'class1'})
      expect(parser.job_definition.parse("[JOB2] = Q2 Class2")).to eq({job: 'JOB2', queue: 'Q2', class: 'Class2'})
    end

    it 'parses connections' do
      expect(parser.connections.parse("[job]:out -> in:[job2]")).to eq({source: {job: 'job', port: 'out'}, tgt_job: {job: 'job2'}, tgt_port: {port: 'in'}, next: [], split: []})
      expect(parser.connections.parse("[job]:out -> in:[job2]:out -> in2:[job3]")).to eq({source: {job: 'job', port: 'out'}, tgt_job: {job: 'job2'}, tgt_port: {port: 'in'}, next: [{src_port: {port: 'out'}, tgt_port: {port: 'in2'}, tgt_job: {job: 'job3'}}], split: []})
      expect(parser.connections.parse("[job]:out -> in:[job2] + in:[job3]")).to eq({source: {job: 'job', port: 'out'}, tgt_job: {job: 'job2'}, tgt_port: {port: 'in'}, next: [], split: [{job: 'job3', port: 'in'}]})
      expect(parser.connections.parse("[job]:out -> in:[job2]:out -> x:[job1] + y:[job2] + z:[job3]")).to eq({source: {job: 'job', port: 'out'}, tgt_job: {job: 'job2'}, tgt_port: {port: 'in'}, next: [{src_port: {port: 'out'}, tgt_port: {port: 'x'}, tgt_job: {job: 'job1'}}], split: [{port: 'y', job: 'job2'}, {port: 'z', job: 'job3'}]})
    end

    it 'parses initial data' do
      expect(parser.connections.parse("'foo' -> in:[job]")).to eq({source: {str: 'foo'}, tgt_port: {port: 'in'}, tgt_job: {job: 'job'}, next: [], split: []})
      expect(parser.connections).to parse("'abc' -> in:[job1]:out -> in2:[job2] + in3:[job3]")
    end

    it 'parses graph inports and outports' do
      expect(parser.connections).to parse('@:input -> in:[job]')
      expect(parser.connections).to parse('[job]:out -> out:@')
      expect(parser.connections).not_to parse('[job]:out -> @:input -> in:[job]')
      expect(parser.connections).to parse('[job]:out -> output:@:input -> in:[job]')
    end

    it 'parses strings' do
      expect(parser.string).not_to parse("abc")
      expect(parser.string).not_to parse('"abc"')
      expect(parser.string.parse("'foo bar'")).to eq({str: 'foo bar'})
      expect(parser.string.parse("'foo \"bar\"'")).to eq({str: 'foo "bar"'})
      expect(parser.string.parse("'foo \"bar\"'")).to eq({str: 'foo "bar"'})
      expect(parser.string.parse("'foo\nbar'")).to eq({str: "foo\nbar"})
      expect(parser.string.parse("'foo\\'bar'")).to eq({str: "foo\\'bar"})
    end

    it 'parses multiple lines' do
      expect(parser).to parse %Q{
        # parse this
        [job3] = testq Foo::Bar # comment
        'bar' -> foo:[job3]
        [job]:out -> in:[job2] + in:[job3]
        [job]:out -> in2:[job2]:out2 -> in3:[job3]
      }
    end
  end

  context 'transformation rules' do
    let(:transform) { SideJob::Parser::Transform.new }
    it 'unescapes strings' do
      expect(transform.apply(str: "foo\\'bar")).to eq({str: "foo'bar"})
    end
  end

  describe '.parse' do
    it 'can successfully parse empty graphs' do
      expect(SideJob::Parser.parse('')).to eq({'jobs' => {}})
      expect(SideJob::Parser.parse('# comment')).to eq({'jobs' => {}})
    end

    it 'can successfully parse jobs' do
      expect(SideJob::Parser.parse("[Job] = q c")).to eq({'jobs' => {'Job' => {'queue' => 'q', 'class' => 'c'}}})
    end

    it 'can successfully parse jobs and connections' do
      expect(SideJob::Parser.parse(%Q{
        # parse this
        [Job1] = testq Foo::Bar
        'hello world' -> port1:[Job1] # comment
        [Job2] = testq Foo::Baz
        'val' -> port2:[Job2]
        [Job1]:out -> in:[Job2]:out -> in2:[Job1]:out2 -> in3:[Job1] + in4:[Job1]
        [Job1]:out2 -> in2:[Job2] + x:[Job1]
      })).to eq({'jobs' => {
        'Job1' => {'queue' => 'testq', 'class' => 'Foo::Bar', 'init' => { 'port1' => ['hello world'] }, 'connections' => {'out' => [{'job' => 'Job2', 'port' => 'in'}], 'out2' => [{'job' => 'Job1', 'port' => 'in3'}, {'job' => 'Job1', 'port' => 'in4'}, {'job' => 'Job2', 'port' => 'in2'}, {'job' => 'Job1', 'port' => 'x'}]}},
        'Job2' => {'queue' => 'testq', 'class' => 'Foo::Baz', 'init' => { 'port2' => ['val'] }, 'connections' => {'out' => [{'job' => 'Job1', 'port' => 'in2'}]}},
      }})
    end

    it 'can specify graph inports and outports' do
      expect(SideJob::Parser.parse("[Job] = q c\n@:myIn -> in:[Job]:out -> myOut:@")).to eq({'jobs' => {'Job' => {'queue' => 'q', 'class' => 'c'}}, 'inports' => {'myIn' => [{'job' => 'Job', 'port' => 'in'}]}, 'outports' => {'myOut' => [{'job' => 'Job', 'port' => 'out'}]}})
    end

    it 'raises error if job is defined multiple times' do
      expect { SideJob::Parser.parse("[Job] = q c\n[Job] = q c") }.to raise_error
    end

    it 'raises error if connection refers to non-existent job' do
      expect { SideJob::Parser.parse("[Job]:out -> in:[Job]") }.to raise_error
    end
  end

  describe '.unparse' do
    it 'can successfully unparse empty graphs' do
      expect(SideJob::Parser.unparse({'jobs' => {}})).to eq('')
    end

    it 'can successfully unparse jobs' do
      expect(SideJob::Parser.unparse({'jobs' => {'Job' => {'queue' => 'q', 'class' => 'c', 'init' => { 'port1' => ["va'l"] }}, 'Job1' => {'queue' => 'r', 'class' => 'd', 'init' => { 'port2' => ['val2'] }}}})).to eq("[Job] = q c\n'va\\'l' -> port1:[Job]\n[Job1] = r d\n'val2' -> port2:[Job1]")
    end

    it 'can successfully unparse connections' do
      expect(SideJob::Parser.unparse({'jobs' => {
                 'Job1' => {'queue' => 'testq', 'class' => 'Foo::Bar', 'connections' => {'out' => [{'job' => 'Job1', 'port' => 'in'}], 'out2' => [{'job' => 'Job1', 'port' => 'in2'}, {'job' => 'Job1', 'port' => 'in3'}]}},
      }})).to eq("[Job1] = testq Foo::Bar\n[Job1]:out -> in:[Job1]\n[Job1]:out2 -> in2:[Job1] + in3:[Job1]")
    end

    it 'can successfully unparse inports and outports' do
      expect(SideJob::Parser.unparse({'jobs' => {'Job' => {'queue' => 'q', 'class' => 'c'}}, 'inports' => {'myIn' => [{'job' => 'Job', 'port' => 'in'}]}, 'outports' => {'myOut' => [{'job' => 'Job', 'port' => 'out'}]}})).to eq("[Job] = q c\n@:myIn -> in:[Job]\n[Job]:out -> myOut:@")
    end
  end
end
