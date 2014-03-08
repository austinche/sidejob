class TestSum3Worker < SideJob::Graph
  def graph
    "
[Sum1] = test TestSum
[Sum2] = test TestSum
[Wait] = test TestWait total:1

@:START -> READY:[Sum1]
@:X -> IN:[Sum1]
@:Y -> IN:[Sum1]
[Sum1]:SUM -> IN:[Sum2]
@:Z -> IN:[Sum2]

[Sum2]:SUM -> OUT:@
[Sum1]:SUM -> IN:[Wait]
[Wait]:READY -> READY:[Sum2]
"
  end
end
