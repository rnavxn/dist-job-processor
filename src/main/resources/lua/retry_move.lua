-- KEYS[1] = retry_queue
-- KEYS[2] = job_queue
-- ARGV[1] = jobId

local removed = redis.call('ZREM', KEYS[1], ARGV[1])

if removed == 1 then
    redis.call('RPUSH', KEYS[2], ARGV[1])
    return 1
end

return 0