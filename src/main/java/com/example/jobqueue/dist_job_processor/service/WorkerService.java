package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.model.Job;
import com.google.gson.Gson;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.args.ListDirection;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
@RequiredArgsConstructor
@Slf4j // This gives us a 'log' object instead of using System.out
public class WorkerService {

    private final JedisPool jedisPool;
    private final Gson gson = new Gson();
    private static final String QUEUE_NAME = "job_queue";
    private static final String PROCESSING_QUEUE = "processing_queue"; // New list!

    // Create a Thread Pool with 5 "Workers"
    private final ExecutorService threadPool = Executors.newFixedThreadPool(5);

    // Reduced to Half a second to poll faster!
    @Scheduled(fixedDelay = 500)
    public void pollAndProcess() {
        try (Jedis jedis = jedisPool.getResource()) {

            // ATOMIC MOVE: Take from 'job_queue' and put into 'processing_queue'
            // If the app crashes now, the job is safely in the 'processing_queue'
            String jsonJob = jedis.lmove(QUEUE_NAME, PROCESSING_QUEUE,
                    ListDirection.LEFT, ListDirection.RIGHT);

            if (jsonJob != null) {
                // Hand the job to the Thread Pool and immediately look for the next job
                threadPool.submit(() -> {
                    processTask(jsonJob);
                });
            }
        } catch (Exception e) {
            log.error("Worker encountered an error: {}", e.getMessage());
        }
    }

    private void processTask(String jsonJob) {
        Job job = gson.fromJson(jsonJob, Job.class);
        try {
            log.info("Thread {} starting Job: {}", Thread.currentThread().getName(), job.getId());
            Thread.sleep(5000); // Simulate a long 5-second task

            // Cleanup after success
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.lrem("processing_queue", 1, jsonJob);
            }
            log.info("Finished: {}", job.getId());
        } catch (Exception e) {
            log.error("Fatal error during task execution");
        }
    }
}