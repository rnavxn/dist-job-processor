package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.model.Job;
import com.google.gson.Gson;
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
public class ReaperService {

    private final JedisPool jedisPool;
    private final Gson gson = new Gson();

    // Run every 2 minutes (120,000ms) to check for zombies
    @Scheduled(fixedRate = 60000)
    public void reclaimStuckJobs() {
        try (Jedis jedis = jedisPool.getResource()) {
            List<String> processingJobs = jedis.lrange("processing_queue", 0, -1);
            long now = System.currentTimeMillis();

            for (String jsonJob : processingJobs) {
                try {
                    Job job = gson.fromJson(jsonJob, Job.class);

                    // Logic: If it has started and been running > 5 mins
                    // OR if it's been sitting in processing > 2 mins without starting
                    boolean isStuck = false;

                    if (job.getStartedAt() != null) {
                        if (now - job.getStartedAt() > 300000) isStuck = true;
                    } else if (now - job.getCreatedAt() > 120000) {
                        isStuck = true;
                    }

                    if (isStuck) {
                        log.warn("REAPER: Job {} is stuck. Reclaiming...", job.getId());

                        // ATOMIC RECLAIM:
                        // We ONLY push back if we successfully remove the EXACT string.
                        // This prevents the 3,000 ghost jobs loop from previous bug.
                        long removed = jedis.lrem("processing_queue", 1, jsonJob);
                        if (removed > 0) {
                            jedis.rpush("job_queue", jsonJob);
                            log.info("REAPER: Successfully returned job {} to waiting queue", job.getId());
                        }
                    }
                } catch (Exception e) {
                    log.error("REAPER: Failed to parse job JSON, skipping item", e);
                }
            }
        }
    }
}
