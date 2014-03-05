require 'bundler/setup'
require 'sidekiq/web'
require 'sidejob/server'
run Sidekiq::Web
