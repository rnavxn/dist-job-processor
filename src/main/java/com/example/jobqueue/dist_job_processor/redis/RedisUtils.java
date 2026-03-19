package com.example.jobqueue.dist_job_processor.redis;

import redis.clients.jedis.Jedis;

public class RedisUtils {

    public static void safeUnlock(Jedis jedis, String lockKey, String lockValue) {
        String currentValue = jedis.get(lockKey);

        if (lockValue.equals(currentValue)) {
            jedis.del(lockKey);
        }
    }
}
