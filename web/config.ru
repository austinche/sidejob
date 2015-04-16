require 'bundler/setup'
require_relative './app.rb'
require 'rack/cors'

if ENV['SIDEJOB_API_CORS']
  use Rack::Cors do
    allow do
      origins ENV['SIDEJOB_API_CORS'].split(' ')
      resource '*', methods: [:get, :post, :put, :delete, :options], headers: :any
    end
  end
end

run SideJob::Web
