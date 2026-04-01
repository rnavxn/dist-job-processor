package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.config.JobConstants;
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
import java.util.Map;

@Profile("maintenance")
@Service
@RequiredArgsConstructor
@Slf4j
public class ReaperService {

    private final JedisPool jedisPool;
    private final JobPersistenceService persistenceService;
    private final RedisScriptManager scriptManager;

    @Scheduled(fixedRate = JobConstants.REAPER_INTERVAL_MS)
    public void reclaimStuckJobs() {

        try (Jedis jedis = jedisPool.getResource()) {

            // Replace full scan with batched LRANGE (0, BATCH_SIZE) for scalability
            List<String> processingJobs = jedis.lrange(RedisKeys.PROCESSING_QUEUE, 0, JobConstants.REAPER_BATCH_SIZE);
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
                        if (runningTime > JobConstants.STUCK_JOB_TIMEOUT_MS) {
                            isStuck = true;
                            stuckReason = "Running for " + (runningTime / 1000) + " seconds";
                        }
                    }
                    // Case 2: Job never started, in queue too long
                    else {
                        long waitingTime = now - createdAt;
                        if (waitingTime > JobConstants.QUEUE_STUCK_TIMEOUT_MS) {
                            isStuck = true;
                            stuckReason = "Never started, waiting for " + (waitingTime / 1000) + " seconds";
                        }
                    }

                    if (isStuck) {
                        log.warn("REAPER: Job {} is stuck. Reclaiming...", jobId);

                        // ========== STEP 1: Atomic Redis Reclaim ==========
                        // Remove from processing queue atomically
                        Object result = jedis.eval(
                                scriptManager.get("processing_to_job"),
                                List.of(RedisKeys.PROCESSING_QUEUE, RedisKeys.JOB_QUEUE),
                                List.of(jobId)
                        );

                        if ((Long) result == 1L) {

                            // ========== STEP 2: Update Redis metadata ==========
                            jedis.hset(RedisKeys.jobKey(jobId), "status", JobStatus.QUEUED.name());
                            jedis.hdel(RedisKeys.jobKey(jobId), "startedAt");

                            // ========== STEP 3: Update PostgreSQL ==========
                            // The job is being recovered. Database needs to know:
                            // 1. Status back to QUEUED (so it can be picked up again)
                            // 2. We need to record that a recovery happened
                            // 3. Clear started_at since job will restart fresh
                            try {
                                persistenceService.recoverStuckJob(jobId, stuckReason);
                                log.info("REAPER: Updated PostgreSQL for recovered job {}", jobId);
                            } catch (Exception e) {
                                // NOTE: If PostgreSQL update fails, reconciliation service will correct state later
                                log.error("CRITICAL: Failed to update PostgreSQL for recovered job {}: {}",
                                        jobId, e.getMessage());
                            }

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
