require 'spec_helper'

describe 'Channels'do
  it 'Global pubsub via channels works correctly' do
    job1 = SideJob.queue('testq', 'TestSum')
    job2 = SideJob.queue('testq', 'TestSum')
    job3 = SideJob.queue('testq', 'TestSum')
    job4 = SideJob.queue('testq', 'TestSum')

    job1.input(:ready).channels = [ '/test/ready' ]
    job1.input(:in).channels = ['/test/in']
    job1.output(:sum).channels = ['/test/chan1']

    job2.input(:ready).channels = [ '/test/ready' ]
    job2.input(:in).channels = ['/test/chan1']
    job2.output(:sum).channels = ['/test/chan2']

    job3.input(:ready).channels = [ '/test/ready' ]
    job3.input(:in).channels = ['/test/chan1']
    job3.output(:sum).channels = ['/test/chan2']

    job4.input(:ready).channels = [ '/test/ready' ]
    job4.input(:in).channels = ['/test/chan2']

    [1,2,4].each {|x| SideJob.publish '/test/in', x}
    SideJob.publish '/test/ready', true

    job1.run_inline
    job2.run_inline
    job3.run_inline
    job4.run_inline

    expect(job4.status).to eq 'completed'
    expect(job4.output(:sum).read).to eq(14)
  end
end
