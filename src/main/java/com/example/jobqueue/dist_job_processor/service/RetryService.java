package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.config.JobConstants;
import com.example.jobqueue.dist_job_processor.metrics.JobMetrics;
import com.example.jobqueue.dist_job_processor.model.JobStatus;
import com.example.jobqueue.dist_job_processor.redis.RedisKeys;
import com.example.jobqueue.dist_job_processor.redis.RedisScriptManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;

@Profile("worker")
@Service
@RequiredArgsConstructor
@Slf4j
public class RetryService {

    private final JedisPool jedisPool;
    private final RedisScriptManager scriptManager;
    private final JobPersistenceService persistenceService;
    private final JobMetrics jobMetrics;

    @Scheduled(fixedRate = JobConstants.RETRY_SERVICE_INTERVAL_MS)
    public void processRetryQueue() {

        // This scheduler runs periodically to move jobs whose retry delay has expired
        // from the retry_queue back to the main job_queue for re-processing.
        try (Jedis jedis = jedisPool.getResource()) {
            long now = System.currentTimeMillis();

            // Fetch all jobs whose retry timestamp (score) is <= current time
            // These jobs are ready to be retried
            List<String> readyJobs = jedis.zrangeByScore(
                    RedisKeys.RETRY_QUEUE,
                    0,
                    now,
                    0,
                    JobConstants.RETRY_BATCH_SIZE
            );

            for (String jobId : readyJobs) {

                // ========== STEP 1: Atomic Redis Move ==========
                // Move from RETRY_QUEUE to JOB_QUEUE atomically
                // This prevents duplicate retries if multiple schedulers run
                Object ob = jedis.eval(
                        scriptManager.get("retry_to_job"),
                        List.of(RedisKeys.RETRY_QUEUE, RedisKeys.JOB_QUEUE),
                        List.of(jobId)
                );

                jobMetrics.getJobsRetried().increment();

                if ((Long) ob == 1L) {
                    log.info("Scheduler picked job {}", jobId);

                    // ========== STEP 2: Update PostgreSQL ==========
                    // The job is being retried. Database needs to know:
                    // 1. Status back to QUEUED (so it can be picked up)
                    // 2. But we DON'T increment attempts here — that was done in WorkerService.handleFailure()
                    // 3. We just need to ensure status is QUEUED
                    //
                    // Here, what's the current status in PostgreSQL?
                    // It should be QUEUED already from WorkerService.handleFailure()
                    // So why update?
                    // Because we need to log the retry attempt in database.

                    try {
                        // Record retry event in PostgreSQL
                        persistenceService.recordRetry(jobId);

                        log.info("Recorded retry for job {} in PostgreSQL", jobId);

                    } catch (Exception e) {
                        log.error("CRITICAL: Failed to record retry in PostgreSQL for job {}: {}", jobId, e.getMessage());
                        // We don't rollback Redis move because job is already in queue
                    }

                    // ========== STEP 3: Update Redis Status ==========
                    // Mark job as QUEUED in Redis (already QUEUED from handleFailure, but be safe)
                    jedis.hset(RedisKeys.jobKey(jobId), "status", JobStatus.QUEUED.name());

                    log.info("Job {} moved from retry queue to main queue", jobId);
                }
            }
        }
    }

}
