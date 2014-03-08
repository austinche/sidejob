require 'bundler/setup'
require 'sidekiq/web'
require 'sidejob'
run Sidekiq::Web
