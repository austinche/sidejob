require File.expand_path('../lib/sidejob/version', __FILE__)

Gem::Specification.new do |s|
  s.name        = 'sidejob'
  s.version     = SideJob::VERSION
  s.authors     = ['Austin Che']
  s.email       = ['austin@ginkgobioworks.com']
  s.summary     = 'Use SideJob to run sidekiq jobs with a flow-based model'

  s.files = `git ls-files`.split($/)

  s.require_paths = ['lib']

  s.add_dependency 'sidekiq', '~>3.2.5'

  # development
  s.add_development_dependency 'pry'
  s.add_development_dependency 'rspec'
  s.add_development_dependency 'simplecov'
  s.add_development_dependency 'yard'
end
