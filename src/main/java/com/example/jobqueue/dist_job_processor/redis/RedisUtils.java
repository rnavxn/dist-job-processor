package com.example.jobqueue.dist_job_processor.redis;

import redis.clients.jedis.Jedis;

import java.util.List;

public class RedisUtils {

    public static void safeUnlock(Jedis jedis, String lockKey, String lockValue) {
        // Atomic lock removal
        // Since this method is static, we use lua script directly
        String script = "if redis.call('get', KEYS[1]) == ARGV[1] then " +
                        "    return redis.call('del', KEYS[1]) " +
                        "else " +
                        "    return 0 " +
                        "end";

        jedis.eval(script, List.of(lockKey), List.of(lockValue));
    }
}
