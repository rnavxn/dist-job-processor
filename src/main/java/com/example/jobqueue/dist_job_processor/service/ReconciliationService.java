package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.config.JobConstants;
import com.example.jobqueue.dist_job_processor.entity.JobEntity;
import com.example.jobqueue.dist_job_processor.metrics.JobMetrics;
import com.example.jobqueue.dist_job_processor.model.JobStatus;
import com.example.jobqueue.dist_job_processor.redis.RedisKeys;
import com.example.jobqueue.dist_job_processor.redis.RedisScriptManager;
import com.example.jobqueue.dist_job_processor.redis.RedisUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Profile("maintenance")
@Service
@RequiredArgsConstructor
@Slf4j
public class ReconciliationService {

    private final JedisPool jedisPool;
    private final JobPersistenceService persistenceService;
    private final JobMetrics jobMetrics;

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
        int corruptedCount = 0;

        for (JobEntity job : queuedJobs) {
            String jobId = job.getId();

            try {
                // ========== STEP 2: Check if job exists anywhere in Redis ==========
                // We must ensure job is not already present in any queue

                boolean existsInQueue =
                        jedis.lpos(RedisKeys.JOB_QUEUE, jobId) != null ||
                        jedis.lpos(RedisKeys.PROCESSING_QUEUE, jobId) != null ||
                        jedis.zscore(RedisKeys.RETRY_QUEUE, jobId) != null ||
                        jedis.lpos(RedisKeys.DEAD_LETTER_QUEUE, jobId) != null;

                // Check if Redis metadata exists and has required fields
                Map<String, String> metadata = jedis.hgetAll(RedisKeys.jobKey(jobId));
                boolean metadataValid = metadata != null &&
                        !metadata.isEmpty() &&
                        metadata.containsKey("id") &&
                        metadata.containsKey("type") &&
                        metadata.containsKey("payload");

                // ========== STEP 3: If missing → re-enqueue ==========
                // Fix if missing from queue OR metadata is corrupted
                if (!existsInQueue || !metadataValid) {
                    // Recreate metadata from PostgreSQL if missing or corrupted
                    if (!metadataValid) {
                        Map<String, String> freshMetadata = getFreshMetadata(job);
                        jedis.hset(RedisKeys.jobKey(jobId), freshMetadata);
                        log.warn("RECON: Recreated corrupted metadata for job {}", jobId);
                        corruptedCount++;

                        jobMetrics.getReconciliationCorrupted().increment();
                    }

                    // Push to queue if missing
                    if (!existsInQueue) {
                        String redisStatus = metadata.get("status");
                        String dbStatus = job.getStatus().name();

                        // If Redis says COMPLETED but PostgreSQL says QUEUED/PROCESSING
                        if ("COMPLETED".equals(redisStatus) && !"COMPLETED".equals(dbStatus)) {
                            log.warn("RECON: Status mismatch - Job {} is COMPLETED in Redis but {} in DB. Fixing DB.",
                                    jobId, dbStatus);

                            // Update PostgreSQL to COMPLETED
                            persistenceService.markCompleted(jobId);

                            // Also clean up any lingering queue entries
                            jedis.lrem(RedisKeys.JOB_QUEUE, 1, jobId);
                            jedis.lrem(RedisKeys.PROCESSING_QUEUE, 1, jobId);
                            jedis.zrem(RedisKeys.RETRY_QUEUE, jobId);

                            continue;  // Skip re-enqueue logic
                        }
                        
                        jedis.rpush(RedisKeys.JOB_QUEUE, jobId);
                        log.warn("RECON: Re-enqueued missing job {}", jobId);
                        reenqueuedCount++;

                        jobMetrics.getReconciliationMissing().increment();
                    }
                }

            } catch (Exception e) {
                // We don’t fail the whole reconciliation because of one job
                log.error("RECON: Failed processing job {}", jobId, e);
            }
        }

        if (reenqueuedCount > 0 || corruptedCount > 0) {
            log.info("Reconciled {} missing jobs, fixed {} corrupted jobs", reenqueuedCount, corruptedCount);
        }
    }

    private static @NonNull Map<String, String> getFreshMetadata(JobEntity job) {
        Map<String, String> freshMetadata = new HashMap<>();
        freshMetadata.put("id", job.getId());
        freshMetadata.put("type", job.getType().name());
        freshMetadata.put("payload", job.getPayload());
        freshMetadata.put("createdAt", String.valueOf(job.getCreatedAt()));
        freshMetadata.put("attempts", String.valueOf(job.getAttempts()));
        freshMetadata.put("status", JobStatus.QUEUED.name());
        return freshMetadata;
    }
}
