class TestSum
  include SideJob::Worker
  register(
      inports: {
          ready: {},
          in: {},
      },
      outports: {
          sum: {},
      }
  )
  def perform
    suspend unless input(:ready).data?
    sum = input(:in).inject(&:+)
    output(:sum).write sum
  end
end
