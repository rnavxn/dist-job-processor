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

    // Run every minute to look for stuck jobs
    @Scheduled(fixedRate = 60000)
    public void reclaimStuckJobs() {
        try (Jedis jedis = jedisPool.getResource()) {
            // Get all jobs currently being processed
            List<String> processingJobs = jedis.lrange("processing_queue", 0, -1);
            long now = System.currentTimeMillis();

            for (String jsonJob : processingJobs) {
                Job job = gson.fromJson(jsonJob, Job.class);

                // If the job has been "Processing" for more than 5 minutes
                // Note : Adjust 300000ms to 30000ms for quick testing
                if (now - job.getStartedAt() > 300000) {
                    log.warn("REAPER: Job {} is stuck. Reclaiming...", job.getId());

                    // Remove it from processing and put it back in the main queue
                    jedis.lrem("processing_queue", 1, jsonJob);
                    jedis.rpush("job_queue", jsonJob);
                }
            }
        }
    }
}