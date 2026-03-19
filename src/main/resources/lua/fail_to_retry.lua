-- KEYS[1] = processing_queue
-- KEYS[2] = retry_queue
-- ARGV[1] = jobId
-- ARGV[2] = retryTimestamp

local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])

if removed == 1 then
	redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
	return 1
end

return 0