require 'sinatra'
require 'sidejob'

# Provide a limited web API to SideJob methods
class SideJob::Web < Sinatra::Base
  before do
    content_type :json
  end

  # for CORS
  options '/' do
    200
  end

  # provide some limited info for now
  get '/' do
    { version: '1.0' }.to_json
  end

  # queue a new job
  post '/jobs' do
    api_call do |params|
      queue = params['queue'] or return { error: 'Missing queue' }
      klass = params['class'] or return { error: 'Missing class' }

      job = SideJob.queue(queue, klass, args: params['args'],
                          parent: SideJob.find(params['parent']), name: params['name'],
                          by: params['by'], inports: params['inports'], outports: params['outports'])
      { job: job.id }
    end
  end

  # gets job state and port info
  get '/jobs/:job' do
    job_api do |job, params|
      { job: job.id, state: job.state, inports: ports_info(job.inports), outports: ports_info(job.outports) }
    end
  end

  # sets job state
  post '/jobs/:job/state' do
    job_api do |job, params|
      job.set(params)
      { job: job.id, state: job.state }
    end
  end

  # delete a job
  delete '/jobs/:job' do
    job_api do |job, params|
      { job: job.id, delete: job.delete }
    end
  end

  # set job ports
  post '/jobs/:job/ports' do
    job_api do |job, params|
      job.inports = params['inports'] if params['inports']
      job.outports = params['outports'] if params['outports']
      nil
    end
  end

  # port operations
  post '/jobs/:job/*ports/:port/:operation' do |job_id, type, port_name, operation|
    job_api do |job, params|
      case type
        when 'in'
          port = job.input(port_name)
        when 'out'
          port = job.output(port_name)
        else
          raise 'Invalid port type'
      end

      case operation
        # read one value
        when 'read'
          data = port.read
          if data == SideJob::Port::None
            {}
          else
            { data: data }
          end

        # read all and return an array of data
        when 'entries'
          { entries: port.entries }

        # port write
        when 'write'
          if params['list']
            list = params['list']
          else
            list = [params['data']]
          end
          list.each {|x| port.write(x)}
          nil
      end
    end
  end

  # run a job
  post '/jobs/:job/run' do
    job_api do |job, params|
      job.run(force: params['force'], at: params['at'], wait: params['wait'])
      nil
    end
  end

  # terminate a job
  post '/jobs/:job/terminate' do
    job_api do |job, params|
      job.terminate(recursive: params['recursive'])
      nil
    end
  end

  # lock job
  post '/jobs/:job/lock' do
    job_api do |job, params|
      { token: job.lock(params['ttl']) }
    end
  end

  # refresh lock
  post '/jobs/:job/refresh_lock' do
    job_api do |job, params|
      { refresh: job.refresh_lock(params['ttl']) }
    end
  end

  # unlock job
  post '/jobs/:job/unlock' do
    job_api do |job, params|
      { unlock: job.unlock(params['token']) }
    end
  end

  # adopt another job
  post '/jobs/:job/adopt' do
    job_api do |job, params|
      orphan = SideJob.find(params[:child])
      raise 'Child job does not exist' unless orphan
      job.adopt(orphan, params['name'])
      nil
    end
  end

  # disown a job
  post '/jobs/:job/disown' do
    job_api do |job, params|
      job.disown(params['name'])
      nil
    end
  end

  # return all logs
  get '/logs' do
    api_call do |params|
      SideJob.logs(clear: params['clear'] || false)
    end
  end

  # add a log entry
  post '/logs' do
    api_call do |params|
      SideJob.log(params['entry'])
    end
  end

  private

  def job_api(&block)
    job = SideJob.find(params['job'])
    halt 422, { error: "Job #{params['job']} does not exist" }.to_json unless job
    api_call do |params|
      result = yield(job, params)
      job.run(parent: params['run']['parent'], force: params['run']['force']) if params['run']
      result
    end
  end

  def api_call(&block)
    begin
      request.body.rewind
      params = JSON.parse(request.body.read) rescue {}
      SideJob.log_context(params['log_context'] || {}) do
        (yield params).to_json
      end
    rescue => e
      puts e.inspect
      puts e.backtrace
      halt 422, { error: e.to_s }.to_json
    end
  end

  # @param ports [Array<SideJob::Port>]
  def ports_info(ports)
    ports.each_with_object({}) do |port, hash|
      hash[port.name] = {size: port.size}
      hash[port.name]['default'] = port.default if port.default?
    end
  end
end
