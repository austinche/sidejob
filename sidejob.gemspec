require File.expand_path('../lib/sidejob/version', __FILE__)

Gem::Specification.new do |s|
  s.name        = 'sidejob'
  s.version     = SideJob::VERSION
  s.authors     = ['Ginkgo BioWorks']
  s.summary     = 'Use SideJob to run sidekiq jobs with a flow-based model'

  s.files = Dir['**/*']

  s.require_paths = ['lib']

  s.add_dependency 'sidekiq'

  s.add_development_dependency 'execjs' # fbp parser

  # sidekiq/web
  s.add_development_dependency 'puma'
  s.add_development_dependency 'sinatra'

  # development
  s.add_development_dependency 'pry'
  s.add_development_dependency 'rspec'
end
