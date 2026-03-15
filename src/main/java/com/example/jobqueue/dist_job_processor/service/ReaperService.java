package com.example.jobqueue.dist_job_processor.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.args.ListDirection;

@Service
@RequiredArgsConstructor
@Slf4j
public class ReaperService {

    private final JedisPool jedisPool;

    // Run every 30 seconds to look for stuck jobs
    @Scheduled(fixedRate = 30000)
    public void reclaimStuckJobs() {
        try (Jedis jedis = jedisPool.getResource()) {
            // To check this idealogy
            // For now this project, we'll simply move EVERYTHING from
            // processing back to the main queue if the worker died.

            // LMOVE from processing_queue -> job_queue
            String reclaimedJob;
            while ((reclaimedJob = jedis.lmove("processing_queue", "job_queue",
                    ListDirection.LEFT, ListDirection.RIGHT)) != null) {
                log.warn("REAPER: Reclaimed a stuck job: {}", reclaimedJob);
            }
        }
    }
}
