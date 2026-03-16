package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.model.JobStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class ReaperService {

    private final JedisPool jedisPool;

    private static final String PROCESSING_QUEUE = "processing_queue";
    private static final String JOB_QUEUE = "job_queue";
    private static final String JOB_KEY_PREFIX = "job:";

    // Run every 2 minutes (120,000ms) to check for zombies
    @Scheduled(fixedRate = 60000)
    public void reclaimStuckJobs() {

        try (Jedis jedis = jedisPool.getResource()) {

            List<String> processingJobs = jedis.lrange(PROCESSING_QUEUE, 0, -1);
            long now = System.currentTimeMillis();

            for (String jobId : processingJobs) {
                try {
                    Map<String, String> jobData = jedis.hgetAll(JOB_KEY_PREFIX + jobId);

                    if (jobData == null || jobData.isEmpty()) {
                        log.warn("REAPER: Metadata missing for job {}", jobId);
                        continue;
                    }

                    Long startedAt = jobData.get("startedAt") != null
                            ? Long.parseLong(jobData.get("startedAt"))
                            : null;

                    long createdAt = Long.parseLong(jobData.get("createdAt"));

                    // Logic: If it has started and been running > 5 mins
                    // OR if it's been sitting in processing > 2 mins without starting
                    boolean isStuck = false;

                    // running too long
                    if (startedAt != null) {
                        if (now - startedAt > 300000) {
                            isStuck = true;
                        }
                    }
                    // stuck before starting
                    else if (now - createdAt > 120000) {
                        isStuck = true;
                    }

                    if (isStuck) {
                        log.warn("REAPER: Job {} is stuck. Reclaiming...", jobId);

                        // ATOMIC RECLAIM:
                        // We ONLY push back if we successfully remove the EXACT string.
                        // This prevents the 3,000 ghost jobs loop from previous bug.
                        long removed = jedis.lrem(PROCESSING_QUEUE, 1, jobId);

                        if (removed > 0) {
                            jedis.hset(
                                    JOB_KEY_PREFIX + jobId,
                                    "status",
                                    JobStatus.QUEUED.name()
                            );

                            jedis.rpush(JOB_QUEUE, jobId);

                            log.info(
                                    "REAPER: Successfully returned job {} to waiting queue",
                                    jobId
                            );
                        }
                    }
                } catch (Exception e) {
                    log.error("REAPER: Failed while inspecting job {}", jobId, e);
                }
            }
        }
    }
}
