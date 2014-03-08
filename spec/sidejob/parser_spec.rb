require 'spec_helper'

require 'parslet/rig/rspec'
describe SideJob::Parser do
  context 'parse rules' do
    let(:parser) { SideJob::Parser.new }

    it 'parses comments' do
      expect(parser.comment).to parse('#')
      expect(parser.comment).to parse('# hello world')
    end

    it 'parses arguments' do
      expect(parser.job_argument).to parse("arg:string")
      expect(parser.job_argument).to parse("arg:'string'")
      expect(parser.job_argument).to parse("arg:\"string\"")
    end

    it 'parses job definition' do
      expect(parser.job_definition).not_to parse("[job] = q")
      expect(parser.job_definition.parse("[job] = q class1")).to eq({job: 'job', queue: 'q', class: 'class1', args: []})
      expect(parser.job_definition.parse("[JOB2] = Q2 Class2 arg:value")).to eq({job: 'JOB2', queue: 'Q2', class: 'Class2', args: [{name: 'arg', value: 'value'}]})
      expect(parser.job_definition.parse("[job] = q class arg:'hello world' arg2:\"'!@#\"")).to eq({job: 'job', queue: 'q', class: 'class', args: [{name: 'arg', value: 'hello world'}, {name: 'arg2', value: "'!@#"}]})
    end

    it 'parses connections' do
      expect(parser.connections.parse("[job]:out -> in:[job2]")).to eq({src_job: {job: 'job'}, src_port: {port: 'out'}, tgt_job: {job: 'job2'}, tgt_port: {port: 'in'}, next: [], split: []})
      expect(parser.connections.parse("[job]:out -> in:[job2]:out -> in2:[job3]")).to eq({src_job: {job: 'job'}, src_port: {port: 'out'}, tgt_job: {job: 'job2'}, tgt_port: {port: 'in'}, next: [{src_port: {port: 'out'}, tgt_port: {port: 'in2'}, tgt_job: {job: 'job3'}}], split: []})
      expect(parser.connections.parse("[job]:out -> in:[job2] + in:[job3]")).to eq({src_job: {job: 'job'}, src_port: {port: 'out'}, tgt_job: {job: 'job2'}, tgt_port: {port: 'in'}, next: [], split: [{job: 'job3', port: 'in'}]})
      expect(parser.connections.parse("[job]:out -> in:[job2]:out -> x:[job1] + y:[job2] + z:[job3]")).to eq({src_job: {job: 'job'}, src_port: {port: 'out'}, tgt_job: {job: 'job2'}, tgt_port: {port: 'in'}, next: [{src_port: {port: 'out'}, tgt_port: {port: 'x'}, tgt_job: {job: 'job1'}}], split: [{port: 'y', job: 'job2'}, {port: 'z', job: 'job3'}]})
    end

    it 'parses graph inports and outports' do
      expect(parser.connections).to parse('@:input -> in:[job]')
      expect(parser.connections).to parse('[job]:out -> out:@')
      expect(parser.connections).not_to parse('[job]:out -> @:input -> in:[job]')
      expect(parser.connections).to parse('[job]:out -> output:@:input -> in:[job]')
    end

    it 'parses multiple lines' do
      expect(parser).to parse %Q{
        # parse this
        [job3] = testq Foo::Bar foo:bar foo2:"hello world" # comment
        [job]:out -> in:[job2] + in:[job3]
        [job]:out -> in2:[job2]:out2 -> in3:[job3]
      }
    end
  end

  describe '.parse' do
    it 'can successfully parse empty graphs' do
      expect(SideJob::Parser.parse('')).to eq({'jobs' => {}})
      expect(SideJob::Parser.parse('# comment')).to eq({'jobs' => {}})
    end

    it 'can successfully parse jobs' do
      expect(SideJob::Parser.parse("[Job] = q c")).to eq({'jobs' => {'Job' => {'queue' => 'q', 'class' => 'c'}}})
      expect(SideJob::Parser.parse("[Job] = q c arg:val")).to eq({'jobs' => {'Job' => {'queue' => 'q', 'class' => 'c', 'args' => { 'arg' => 'val' }}}})
      expect(SideJob::Parser.parse("[Job] = q c arg1:val1
        [Job1] = r d arg2:val2"
        )).to eq({'jobs' => {'Job' => {'queue' => 'q', 'class' => 'c', 'args' => { 'arg1' => 'val1' }}, 'Job1' => {'queue' => 'r', 'class' => 'd', 'args' => { 'arg2' => 'val2' }}}})
    end

    it 'can successfully parse jobs and connections' do
      expect(SideJob::Parser.parse(%Q{
        # parse this
        [Job1] = testq Foo::Bar arg:"hello world" # comment
        [Job2] = testq Foo::Baz arg2:val
        [Job1]:out -> in:[Job2]:out -> in2:[Job1]:out2 -> in3:[Job1] + in4:[Job1]
        [Job1]:out2 -> in2:[Job2] + x:[Job1]
      })).to eq({'jobs' => {
        'Job1' => {'queue' => 'testq', 'class' => 'Foo::Bar', 'args' => { 'arg' => 'hello world' }, 'connections' => {'out' => [{'job' => 'Job2', 'port' => 'in'}], 'out2' => [{'job' => 'Job1', 'port' => 'in3'}, {'job' => 'Job1', 'port' => 'in4'}, {'job' => 'Job2', 'port' => 'in2'}, {'job' => 'Job1', 'port' => 'x'}]}},
        'Job2' => {'queue' => 'testq', 'class' => 'Foo::Baz', 'args' => { 'arg2' => 'val' }, 'connections' => {'out' => [{'job' => 'Job1', 'port' => 'in2'}]}},
      }})
    end

    it 'can specify graph inports and outports' do
      expect(SideJob::Parser.parse("[Job] = q c\n@:myIn -> in:[Job]:out -> myOut:@")).to eq({'jobs' => {'Job' => {'queue' => 'q', 'class' => 'c'}}, 'inports' => {'myIn' => [{'job' => 'Job', 'port' => 'in'}]}, 'outports' => {'myOut' => [{'job' => 'Job', 'port' => 'out'}]}})
    end

    it 'raises error if argument name is duplicated' do
      expect { SideJob::Parser.parse("[Job] = q c x:1 x:2") }.to raise_error
    end

    it 'raises error if job is defined multiple times' do
      expect { SideJob::Parser.parse("[Job] = q c\n[Job] = q c") }.to raise_error
    end

    it 'raises error if connection refers to non-existent job' do
      expect { SideJob::Parser.parse("[Job]:out -> in:[Job]") }.to raise_error
    end
  end
end
