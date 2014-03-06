# require all workers located in this directory
Dir[File.dirname(__FILE__) + '/*.rb'].each {|file| require "sidejob/workers/#{File.basename(file, '.rb')}"}
