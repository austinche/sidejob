require File.expand_path('../lib/sidejob/version', __FILE__)

Gem::Specification.new do |s|
  s.name        = 'sidejob'
  s.version     = SideJob::VERSION
  s.authors     = ['Austin Che']
  s.email       = ['austin@ginkgobioworks.com']
  s.summary     = 'Run sidekiq jobs with a flow-based programming model'
  s.homepage    = 'https://github.com/austinche/sidejob'
  s.license     = 'MIT'
  s.files = `git ls-files`.split($/)
  s.require_paths = ['lib']

  s.add_runtime_dependency 'sidekiq'

  s.add_development_dependency 'pry'
  s.add_development_dependency 'rspec'
  s.add_development_dependency 'simplecov'
  s.add_development_dependency 'yard'
end
