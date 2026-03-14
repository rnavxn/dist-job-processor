package com.example.jobqueue.dist_job_processor.service;
import com.example.jobqueue.dist_job_processor.model.Job;
import com.google.gson.Gson;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@Service
@RequiredArgsConstructor
@Slf4j // This gives us a 'log' object instead of using System.out
public class WorkerService {

    private final JedisPool jedisPool;
    private final Gson gson = new Gson();
    private static final String QUEUE_NAME = "job_queue";

    // Run every 1 second.
    // fixedDelay means: wait 1s AFTER the previous task finishes.
    @Scheduled(fixedDelay = 1000)
    public void pollAndProcess() {
        try (Jedis jedis = jedisPool.getResource()) {
            // LPOP: Get the first job from the list
            String jsonJob = jedis.lpop(QUEUE_NAME);

            if (jsonJob != null) {
                // 1. Deserialization
                Job job = gson.fromJson(jsonJob, Job.class);

                log.info("Starting Job: {} | Type: {}", job.getId(), job.getType());

                // 2. Simulate the 'Work'
                // In a real app, this would be sending an email or calling an API
                performWork(job);

                log.info("Successfully finished Job: {}", job.getId());
            }
        } catch (Exception e) {
            log.error("Worker encountered an error: {}", e.getMessage());
        }
    }

    private void performWork(Job job) throws InterruptedException {
        // We'll simulate a heavy task by sleeping for 2 seconds
        Thread.sleep(2000);
    }
}
