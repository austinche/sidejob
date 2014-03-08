require 'bundler/setup'
require 'sidejob'

require 'rspec/core'
require 'sidejob/testing'

redis = { namespace: 'sidejob_test' }
Sidekiq.configure_client do |config|
  config.redis = redis
end
Sidekiq.configure_server do |config|
  config.redis = redis
end

require 'sidejob/workers'
Dir[File.dirname(__FILE__) + '/workers/*.rb'].each {|file| require file }

RSpec.configure do |config|
  config.order = 'random'
  config.before(:each) do
    Sidekiq::Worker.clear_all
    SideJob.redis do |conn|
      keys = conn.keys('*') # this is namespaced but flushall is not
      conn.del keys if keys.length > 0
    end
    Sidekiq::Testing.fake!
  end
end
