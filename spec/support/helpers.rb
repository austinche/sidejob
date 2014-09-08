def set_status(job, status)
  SideJob.redis.hset job.redis_key, 'status', status
end
