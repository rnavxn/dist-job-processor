package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.model.JobStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class RetryService {

    private final JedisPool jedisPool;

    private static final String JOB_QUEUE = "job_queue";
    private static final String RETRY_QUEUE = "retry_queue";

    private static final String JOB_KEY_PREFIX = "job:";

    private final String retryScript = loadScript("lua/retry_move.lua");


    @Scheduled(fixedRate = 5000)
    public void processRetryQueue() {

        // This scheduler runs periodically to move jobs whose retry delay has expired
        // from the retry_queue back to the main job_queue for re-processing.
        try (Jedis jedis = jedisPool.getResource()) {
            long now = System.currentTimeMillis();

            // Fetch all jobs whose retry timestamp (score) is <= current time
            // These jobs are ready to be retried
            List<String> readyJobs = jedis.zrangeByScore(RETRY_QUEUE, 0, now, 0, 20);   // limit to 20

            for (String jobId : readyJobs) {
                // Remove job from retry queue
                long removed = jedis.zrem(RETRY_QUEUE, jobId);

                // Atomic move using manual Lua script
                Object ob = jedis.eval(
                        retryScript,
                        List.of(RETRY_QUEUE, JOB_QUEUE),
                        List.of(jobId)
                );

                if ((Long) ob == 1L) {
                    // This ensures safe retry handling using ZREM return value to avoid duplicate requeue

                    // Update job status to reflect it is ready for processing again
                    jedis.hset(JOB_KEY_PREFIX + jobId, "status", JobStatus.QUEUED.name());

                    log.info("Retrying job {}", jobId);
                }
            }
        }
    }

    private String loadScript(String path) {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {
            if (is == null) {
                throw new RuntimeException("Lua script not found: " + path);
            }

            return new String(is.readAllBytes(), StandardCharsets.UTF_8);

        } catch (Exception e) {
            throw new RuntimeException("Failed to load Lua script: " + path, e);
        }
    }

}
