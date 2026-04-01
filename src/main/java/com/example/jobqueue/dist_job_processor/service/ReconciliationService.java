package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.config.JobConstants;
import com.example.jobqueue.dist_job_processor.entity.JobEntity;
import com.example.jobqueue.dist_job_processor.model.JobStatus;
import com.example.jobqueue.dist_job_processor.redis.RedisKeys;
import com.example.jobqueue.dist_job_processor.redis.RedisScriptManager;
import com.example.jobqueue.dist_job_processor.redis.RedisUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

import java.util.List;
import java.util.UUID;

@Profile("maintenance")
@Service
@RequiredArgsConstructor
@Slf4j
public class ReconciliationService {

    private final JedisPool jedisPool;
    private final JobPersistenceService persistenceService;

    /**
     * Runs every x seconds
     *
     * Purpose:
     * - Ensure Redis and PostgreSQL are consistent
     * - Recover jobs that exist in DB but are missing in Redis
     */
    @Scheduled(fixedRate = JobConstants.RECONCILIATION_INTERVAL_MS)
    public void reconcileMissingJobs() {
        // Optional: Add lock if you ever run multiple maintenance containers
        // But for single container, we can skip lock entirely

        String lockKey = "lock:reconciliation";
        String lockValue = UUID.randomUUID().toString();

        try (Jedis jedis = jedisPool.getResource()) {
            // Same lock pattern as your worker
            String lockResult = jedis.set(
                    lockKey,
                    lockValue,
                    SetParams.setParams().nx().ex(JobConstants.LOCK_TTL_SECONDS)
            );

            if (lockResult == null) {
                log.debug("Another maintenance instance is running, skipping");
                return;
            }

            try {
                performReconciliation(jedis);
            } finally {
                RedisUtils.safeUnlock(jedis, lockKey, lockValue);
            }

        } catch (Exception e) {
            log.error("RECON: Failed to run reconciliation cycle", e);
        }
    }

    public void performReconciliation(Jedis jedis) {
        // ========== STEP 1: Fetch QUEUED jobs from PostgreSQL ==========
        // These are jobs that SHOULD be in Redis waiting to be processed
        List<JobEntity> queuedJobs = persistenceService.findQueuedJobs(JobConstants.RECONCILIATION_BATCH_SIZE);

        if (queuedJobs.isEmpty()) {
            return;
        }

        int reenqueuedCount = 0;
        for (JobEntity job : queuedJobs) {
            String jobId = job.getId();

            try {
                // ========== STEP 2: Check if job exists anywhere in Redis ==========
                // We must ensure job is not already present in any queue

                boolean exists =
                        jedis.lpos(RedisKeys.JOB_QUEUE, jobId) != null ||
                        jedis.lpos(RedisKeys.PROCESSING_QUEUE, jobId) != null ||
                        jedis.zscore(RedisKeys.RETRY_QUEUE, jobId) != null ||
                        jedis.lpos(RedisKeys.DEAD_LETTER_QUEUE, jobId) != null;

                // ========== STEP 3: If missing → re-enqueue ==========
                if (!exists) {
                    // Push to queue
                    jedis.rpush(RedisKeys.JOB_QUEUE, jobId);
                    log.warn("RECON: Re-enqueued missing job {}", jobId);
                    reenqueuedCount++;
                }

            } catch (Exception e) {
                // We don’t fail the whole reconciliation because of one job
                log.error("RECON: Failed processing job {}", jobId, e);
            }
        }

        if (reenqueuedCount > 0) {
            log.info("Reconciled {} missing jobs", reenqueuedCount);
        }
    }
}
