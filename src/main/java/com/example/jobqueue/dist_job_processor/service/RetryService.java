package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.model.JobStatus;
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

    private static final String JOB_QUEUE = "job_queue";
    private static final String RETRY_QUEUE = "retry_queue";

    private static final String JOB_KEY_PREFIX = "job:";

    @Scheduled(fixedRate = 5000)
    public void processRetryQueue() {

        // This scheduler runs periodically to move jobs whose retry delay has expired
        // from the retry_queue back to the main job_queue for re-processing.
        try (Jedis jedis = jedisPool.getResource()) {

            long now = System.currentTimeMillis();

            // Fetch all jobs whose retry timestamp (score) is <= current time
            // These jobs are ready to be retried
            List<String> readyJobs = jedis.zrangeByScore(RETRY_QUEUE, 0, now);

            for (String jobId : readyJobs) {

                // Remove job from retry queue
                jedis.zrem(RETRY_QUEUE, jobId);

                // Push job back to main queue so workers can pick it up again
                jedis.rpush(JOB_QUEUE, jobId);

                // Update job status to reflect it is ready for processing again
                jedis.hset(JOB_KEY_PREFIX + jobId, "status", JobStatus.QUEUED.name());

                log.info("Retrying job {}", jobId);
            }
        }
    }
}
