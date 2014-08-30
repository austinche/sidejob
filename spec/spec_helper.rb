require 'bundler/setup'
require 'rspec/core'
require 'pry'
require 'sidejob'
require 'sidejob/testing'

# set default redis to something other than database 0 to avoid accidentally clearing a redis with valuable data
ENV['REDIS_URL'] ||= 'redis://localhost:6379/6'

Dir[File.dirname(__FILE__) + '/workers/*.rb'].each {|file| require file }

RSpec.configure do |config|
  config.order = 'random'
  config.before(:each) do
    SideJob.redis.flushdb
  end
end
