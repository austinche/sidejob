module SideJob
  # Options:
  # Input ports:
  #   in: graph in the language parsed by SideJob::Parser
  # Output ports
  #   out: JSON parsed form of input graph
  class GraphParser
    include SideJob::Worker
    def perform
      graph = input(:in).pop
      output(:out).push_json SideJob::Parser.parse(graph) if graph
    end
  end
end
