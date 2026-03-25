package com.example.jobqueue.dist_job_processor.service;

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
import java.util.Map;

@Profile("worker")
@Service
@RequiredArgsConstructor
@Slf4j
public class ReaperService {

    private final JedisPool jedisPool;

    private final JobPersistenceService persistenceService;

    // Run every 2 minutes (120,000ms) to check for zombies
    @Scheduled(fixedRate = 120000)
    public void reclaimStuckJobs() {

        try (Jedis jedis = jedisPool.getResource()) {

            // Get ALL jobs currently in processing queue
            // NOTE: This scans the whole queue - not scalable for large queues
            // TODO: Add batching for production
            List<String> processingJobs = jedis.lrange(RedisKeys.PROCESSING_QUEUE, 0, -1);
            long now = System.currentTimeMillis();

            for (String jobId : processingJobs) {
                try {
                    Map<String, String> jobData = jedis.hgetAll(RedisKeys.jobKey(jobId));

                    if (jobData == null || jobData.isEmpty()) {
                        log.warn("REAPER: Metadata missing for job {}", jobId);
                        continue;
                    }

                    Long startedAt = jobData.get("startedAt") != null
                            ? Long.parseLong(jobData.get("startedAt"))
                            : null;

                    long createdAt = Long.parseLong(jobData.get("createdAt"));

                    // ========== Check if job is stuck ==========
                    boolean isStuck = false;
                    String stuckReason = null;

                    // Case 1: Job started but running too long
                    if (startedAt != null) {
                        long runningTime = now - startedAt;
                        if (runningTime > 300000) {     // 5 minutes
                            isStuck = true;
                            stuckReason = "Running for " + (runningTime / 1000) + " seconds";
                        }
                    }
                    // Case 2: Job never started, in queue too long
                    else {
                        long waitingTime = now - createdAt;
                        if (waitingTime> 120000) {      // 2 minutes
                            isStuck = true;
                            stuckReason = "Never started, waiting for " + (waitingTime / 1000) + " seconds";
                        }
                    }

                    if (isStuck) {
                        log.warn("REAPER: Job {} is stuck. Reclaiming...", jobId);

                        // ========== STEP 1: Atomic Redis Reclaim ==========
                        // Remove from processing queue atomically
                        long removed = jedis.lrem(RedisKeys.PROCESSING_QUEUE, 1, jobId);

                        if (removed > 0) {

                            // ========== STEP 2: Update PostgreSQL ==========
                            // The job is being recovered. Database needs to know:
                            // 1. Status back to QUEUED (so it can be picked up again)
                            // 2. We need to record that a recovery happened
                            // 3. Clear started_at since job will restart fresh
                            try {
                                persistenceService.recoverStuckJob(jobId, stuckReason);
                                log.info("REAPER: Updated PostgreSQL for recovered job {}", jobId);
                            } catch (Exception e) {
                                // CRITICAL: PostgreSQL update failed
                                // The job is back in Redis queue, but database still shows PROCESSING
                                log.error("CRITICAL: Failed to update PostgreSQL for recovered job {}: {}",
                                        jobId, e.getMessage());
                                // TODO: Add to reconciliation queue
                            }

                            // ========== STEP 3: Update Redis Status ==========
                            jedis.hset(RedisKeys.jobKey(jobId), "status", JobStatus.QUEUED.name());
                            jedis.hdel(RedisKeys.jobKey(jobId), "startedAt");  // Clear started time

                            // ========== STEP 4: Push back to main queue ==========
                            jedis.rpush(RedisKeys.JOB_QUEUE, jobId);

                            log.info("REAPER: Successfully returned job {} to waiting queue", jobId);
                        }
                    }
                } catch (Exception e) {
                    log.error("REAPER: Failed while inspecting job {}", jobId, e);
                }
            }
        }
    }
}
