# require all workers
Dir[File.dirname(__FILE__) + '/workers/*.rb'].each {|file| require "sidejob/workers/#{File.basename(file, '.rb')}"}
