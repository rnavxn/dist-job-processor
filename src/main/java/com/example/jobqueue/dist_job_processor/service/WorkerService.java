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

@Service
@RequiredArgsConstructor
@Slf4j // This gives us a 'log' object instead of using System.out
public class WorkerService {

    private final JedisPool jedisPool;
    private final Gson gson = new Gson();
    private static final String QUEUE_NAME = "job_queue";
    private static final String PROCESSING_QUEUE = "processing_queue"; // New list!


    // Run every 1 second.
    // fixedDelay means: wait 1s AFTER the previous task finishes.
    @Scheduled(fixedDelay = 1000)
    public void pollAndProcess() {
        try (Jedis jedis = jedisPool.getResource()) {

            // ATOMIC MOVE: Take from 'job_queue' and put into 'processing_queue'
            // If the app crashes now, the job is safely in the 'processing_queue'
            String jsonJob = jedis.lmove(QUEUE_NAME, PROCESSING_QUEUE,
                    ListDirection.LEFT, ListDirection.RIGHT);

            if (jsonJob != null) {
                Job job = gson.fromJson(jsonJob, Job.class);
                log.info("Safe-Processing Job: {}", job.getId());

                performWork(job);

                // WORK COMPLETE: Now we can safely remove it from the processing list
                // LREM removes the specific job we just finished
                jedis.lrem(PROCESSING_QUEUE, 1, jsonJob);
                log.info("Successfully finished and cleared Job: {}", job.getId());
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