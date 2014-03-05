require 'sidejob'
require 'sidejob/server_middleware'

Sidekiq.configure_server do |config|
  config.redis = { namespace: 'sidejob' }
  config.server_middleware do |chain|
    chain.add SideJob::ServerMiddleware
  end
  config.client_middleware do |chain|
    chain.add SideJob::ClientMiddleware
  end
end

require 'sidejob/workers/graph'
