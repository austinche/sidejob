class TestSum3Worker < SideJob::Graph
  def graph
    "
INPORT=Sum1.READY:START
INPORT=Sum1.IN:X
INPORT=Sum1.IN:Y
INPORT=Sum2.IN:Z
OUTPORT=Sum2.SUM:OUT
Sum1(test/TestSum) SUM -> IN Sum2(test/TestSum)
Sum1 SUM -> IN Wait(test/TestWait)
'1' -> COUNT Wait READY -> READY Sum2
"
  end
end
