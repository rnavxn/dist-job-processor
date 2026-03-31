-- KEYS[1] = processing_queue
-- KEYS[2] = dead_letter_queue
-- ARGV[1] = jobId

local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])

if removed == 1 then
	redis.call('RPUSH', KEYS[2], ARGV[1])
	return 1
end

return 0