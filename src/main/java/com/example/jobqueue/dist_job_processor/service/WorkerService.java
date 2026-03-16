package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.model.Job;
import com.google.gson.Gson;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.args.ListDirection;


@Service
@RequiredArgsConstructor
@Slf4j // This gives us a 'log' object instead of using System.out
public class WorkerService {

    private final JedisPool jedisPool;
    private final Gson gson = new Gson();
    private static final String QUEUE_NAME = "job_queue";
    private static final String PROCESSING_QUEUE = "processing_queue"; // New list!

    @PostConstruct
    public void startWorkers() {

        int workerCount = 2; // same as your previous thread pool size

        for (int i = 0; i < workerCount; i++) {
            Thread worker = new Thread(this::workerLoop);
            worker.setName("worker-" + i);
            worker.start();
        }

        log.info("Started {} blocking workers", workerCount);
    }


    private void workerLoop() {

        while (true) {

            try (Jedis jedis = jedisPool.getResource()) {

                String jsonJob = jedis.blmove(
                        QUEUE_NAME,
                        PROCESSING_QUEUE,
                        ListDirection.LEFT,
                        ListDirection.RIGHT,
                        0   // block forever
                );

                if (jsonJob != null) {
                    processTask(jsonJob);
                }

            } catch (Exception e) {
                log.error("Worker error: {}", e.getMessage());
            }
        }
    }

    private void processTask(String jsonJob) {
        Job job = gson.fromJson(jsonJob, Job.class);
        try {
            log.info("Thread {} starting Job: {}", Thread.currentThread().getName(), job.getId());
            log.info("Processing: {}", job.getId());
            job.setStartedAt(System.currentTimeMillis());

            Thread.sleep(3000); // Simulate a 3-second task attempt

            // --- A RANDOM FAILURE ---
            // Let's say 20% of jobs fail randomly to test our logic
            if (Math.random() < 0.2) throw new RuntimeException("Simulated Network Error");

           Thread.sleep(7000);

            // SUCCESS : Remove from processing
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.lrem("processing_queue", 1, jsonJob);
            }
            log.info("Finished: {}", job.getId());
        } catch (Exception e) {
            handleFailure(job, jsonJob);
        }
    }

    private void handleFailure(Job job, String jsonJob) {
        try (Jedis jedis = jedisPool.getResource()) {
            // Remove from processing queue regardless
            jedis.lrem("processing_queue", 1, jsonJob);

            job.setAttempts(job.getAttempts() + 1);

            if (job.getAttempts() >= 3) {
                log.error("Job {} failed 3 times. Moving to DLQ!", job.getId());
                jedis.rpush("dead_letter_queue", gson.toJson(job));
            } else {
                log.warn("Job {} failed. Attempt {}. Retrying...", job.getId(), job.getAttempts());
                jedis.rpush("job_queue", gson.toJson(job)); // Put back to try again
            }
        }
    }
}
