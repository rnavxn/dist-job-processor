package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.model.Job;
import com.example.jobqueue.dist_job_processor.model.JobStatus;
import com.example.jobqueue.dist_job_processor.model.JobType;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.args.ListDirection;

import java.util.Map;


@Service
@RequiredArgsConstructor
@Slf4j // This gives us a 'log' object instead of using System.out
public class WorkerService {

    private final JedisPool jedisPool;

    private static final String JOB_QUEUE = "job_queue";
    private static final String PROCESSING_QUEUE = "processing_queue";
    private static final String DEAD_LETTER_QUEUE = "dead_letter_queue";
    private static final String JOB_KEY_PREFIX = "job:";


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

                String jobId = jedis.blmove(
                        JOB_QUEUE,
                        PROCESSING_QUEUE,
                        ListDirection.LEFT,
                        ListDirection.RIGHT,
                        0
                );

                if (jobId != null) {
                    processTask(jobId);
                }

            } catch (Exception e) {
                log.error("Worker error: {}", e.getMessage());
            }
        }
    }


    private void processTask(String jobId) {

        try (Jedis jedis = jedisPool.getResource()){

            // Load job metadata from Redis hash
            Map<String, String> jobData = jedis.hgetAll(JOB_KEY_PREFIX + jobId);

            // If Redis loses metadata (rare but possible)
            if (jobData == null || jobData.isEmpty()) {
                log.error("Job metadata missing for {}", jobId);
                jedis.lrem(PROCESSING_QUEUE, 1, jobId);
                return;
            }

            // Reconstruct Job object from Redis data
            Job job = new Job();
            job.setId(jobData.get("id"));
            job.setType(JobType.valueOf(jobData.get("type")));
            job.setPayload(jobData.get("payload"));
            job.setCreatedAt(Long.parseLong(jobData.get("createdAt")));
            job.setAttempts(Integer.parseInt(jobData.get("attempts")));

            log.info("Thread {} starting Job: {}", Thread.currentThread().getName(), job.getId());

            job.setStartedAt(System.currentTimeMillis());
            jedis.hset(jobKey(jobId), "status", JobStatus.PROCESSING.name());

            // update start time in Redis
            jedis.hset(jobKey(jobId), "startedAt", String.valueOf(job.getStartedAt()));

            Thread.sleep(3000); // Simulate a 3-second task attempt

            // --- A RANDOM FAILURE ---
            // Let's say 20% of jobs fail randomly to test our logic
            if (Math.random() < 0.2)
                throw new RuntimeException("Simulated Network Error");

            Thread.sleep(7000);

            // SUCCESS -> Remove from processing
            jedis.lrem(PROCESSING_QUEUE, 1, jobId);

            // update status
            jedis.hset(jobKey(jobId), "status", JobStatus.COMPLETED.name());

            log.info("Finished: {}", job.getId());

        } catch (Exception e) {
            handleFailure(jobId);
        }
    }

    private String jobKey(String jobId) {
        return JOB_KEY_PREFIX + jobId;
    }

    private void handleFailure(String jobId) {

        try (Jedis jedis = jedisPool.getResource()) {
            // Remove from processing queue regardless
            jedis.lrem(PROCESSING_QUEUE, 1, jobId);

            // increment attempts counter
            long attempts = jedis.hincrBy(jobKey(jobId), "attempts", 1);

            if (attempts >= 3) {
                log.error("Job {} failed 3 times. Moving to DLQ!", jobId);

                jedis.hset(jobKey(jobId), "status", JobStatus.DLQ.name());

                jedis.rpush(DEAD_LETTER_QUEUE, jobId);

            } else {
                log.warn("Job {} failed. Attempt {}. Retrying...", jobId, attempts);

                jedis.hset(jobKey(jobId), "status", JobStatus.QUEUED.name());

                jedis.rpush(JOB_QUEUE, jobId);
            }
        }
    }
}
