package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.entity.JobEntity;
import com.example.jobqueue.dist_job_processor.model.JobStatus;
import com.example.jobqueue.dist_job_processor.redis.RedisKeys;
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
public class ReconciliationService {

    private final JedisPool jedisPool;
    private final JobPersistenceService persistenceService;

    // How many jobs to scan per run (prevents DB overload)
    private static final int BATCH_SIZE = 50;

    /**
     * Runs every 30 seconds
     *
     * Purpose:
     * - Ensure Redis and PostgreSQL are consistent
     * - Recover jobs that exist in DB but are missing in Redis
     */
    @Scheduled(fixedRate = 30000)
    public void reconcileMissingJobs() {

        try (Jedis jedis = jedisPool.getResource()) {

            // ========== STEP 1: Fetch QUEUED jobs from PostgreSQL ==========
            // These are jobs that SHOULD be in Redis waiting to be processed
            List<JobEntity> queuedJobs = persistenceService.findQueuedJobs(BATCH_SIZE);

            for (JobEntity job : queuedJobs) {
                String jobId = job.getId();

                try {
                    // ========== STEP 2: Check if job exists anywhere in Redis ==========
                    // We must ensure job is not already present in any queue

                    boolean existsInJobQueue =
                            jedis.lpos(RedisKeys.JOB_QUEUE, jobId) != null;

                    boolean existsInProcessingQueue =
                            jedis.lpos(RedisKeys.PROCESSING_QUEUE, jobId) != null;

                    boolean existsInRetryQueue =
                            jedis.zscore(RedisKeys.RETRY_QUEUE, jobId) != null;

                    boolean existsInDlq =
                            jedis.lpos(RedisKeys.DEAD_LETTER_QUEUE, jobId) != null;

                    boolean existsAnywhere =
                                existsInJobQueue ||
                                existsInProcessingQueue ||
                                existsInRetryQueue ||
                                existsInDlq;

                    // ========== STEP 3: If missing → re-enqueue ==========
                    if (!existsAnywhere) {
                        // Push job back to main queue
                        jedis.rpush(RedisKeys.JOB_QUEUE, jobId);

                        // Also ensure Redis metadata is consistent
                        jedis.hset(RedisKeys.jobKey(jobId), "status", JobStatus.QUEUED.name());

                        log.warn("RECON: Re-enqueued missing job {}", jobId);
                    }

                } catch (Exception e) {
                    // We don’t fail the whole reconciliation because of one job
                    log.error("RECON: Failed processing job {}", jobId, e);
                }
            }
        } catch (Exception e) {
            // Critical failure: Redis connection or DB failure
            log.error("RECON: Failed to run reconciliation cycle", e);
        }
    }
}
