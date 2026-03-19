package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.model.JobStatus;
import com.example.jobqueue.dist_job_processor.redis.RedisKeys;
import com.example.jobqueue.dist_job_processor.redis.RedisScriptManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class RetryService {

    private final JedisPool jedisPool;
    private final RedisScriptManager scriptManager;

    @Scheduled(fixedRate = 5000)
    public void processRetryQueue() {

        // This scheduler runs periodically to move jobs whose retry delay has expired
        // from the retry_queue back to the main job_queue for re-processing.
        try (Jedis jedis = jedisPool.getResource()) {
            long now = System.currentTimeMillis();

            // Fetch all jobs whose retry timestamp (score) is <= current time
            // These jobs are ready to be retried
            List<String> readyJobs = jedis.zrangeByScore(RedisKeys.RETRY_QUEUE, 0, now, 0, 20);   // limit to 20

            for (String jobId : readyJobs) {

                // Atomic move using manual Lua script
                Object ob = jedis.eval(
                        scriptManager.get("retry_move"),
                        List.of(RedisKeys.RETRY_QUEUE, RedisKeys.JOB_QUEUE),
                        List.of(jobId)
                );

                log.info("Scheduler picked job {}", jobId);

                if ((Long) ob == 1L) {
                    // This ensures safe retry handling using ZREM return value to avoid duplicate requeue

                    // Update job status to reflect it is ready for processing again
                    jedis.hset(RedisKeys.jobKey(jobId), "status", JobStatus.QUEUED.name());

                    log.info("Retrying job {}", jobId);
                }
            }
        }
    }

}
