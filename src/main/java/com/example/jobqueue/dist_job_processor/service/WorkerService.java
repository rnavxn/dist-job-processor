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
    private static final String RETRY_QUEUE = "retry_queue";
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

                // ATOMIC MOVE: Take from JOB_QUEUE and put into PROCESSING_QUEUE
                // If the server crashes now, the job is safely in the PROCESSING_QUEUE
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

        String lockKey = jobKey(jobId) + ":lock";
        String lockValue = java.util.UUID.randomUUID().toString();

        try (Jedis jedis = jedisPool.getResource()){

            // -------- Initial status check --------
            String status = jedis.hget(jobKey(jobId), "status");
            if ("COMPLETED".equals(status)) {
                log.warn("Skipping already completed job {}", jobId);
                jedis.lrem(PROCESSING_QUEUE, 1, jobId);
                return;
            }

            // -------- Acquire lock atomically --------
            String lockResult = jedis.set(
                    lockKey,
                    lockValue,
                    redis.clients.jedis.params.SetParams.setParams().nx().ex(600)
            );

            if (lockResult == null) {
                log.warn("Job {} already locked, skipping", jobId);
                jedis.lrem(PROCESSING_QUEUE, 1, jobId);
                return;
            }

            // -------- Double-check after lock --------
            status = jedis.hget(jobKey(jobId), "status");
            if ("COMPLETED".equals(status)) {
                jedis.lrem(PROCESSING_QUEUE, 1, jobId);
                safeUnlock(jedis, lockKey, lockValue);
                return;
            }

            // -------- Load job --------
            Map<String, String> jobData = jedis.hgetAll(jobKey(jobId));

            if (jobData == null || jobData.isEmpty()) {                 // If Redis loses metadata (rare but possible)
                log.error("Job metadata missing for {}", jobId);
                jedis.lrem(PROCESSING_QUEUE, 1, jobId);
                safeUnlock(jedis, lockKey, lockValue);
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

            // ------- Mark Processing -------
            job.setStartedAt(System.currentTimeMillis());
            jedis.hset(jobKey(jobId), "status", JobStatus.PROCESSING.name());
            jedis.hset(jobKey(jobId), "startedAt", String.valueOf(job.getStartedAt()));

            // ------- Task Execution -------
            Thread.sleep(3000); // Simulate a 3-second task attempt

            // ------- A RANDOM FAILURE -------
            // Let's say 20% of jobs fail randomly to test our logic
            if (Math.random() < 0.2)
                throw new RuntimeException("Simulated Network Error");

            Thread.sleep(7000);

            // Mark COMPLETED
            jedis.hset(jobKey(jobId), "status", JobStatus.COMPLETED.name());
            log.info("Finished: {}", job.getId());

            // ------- Remove from PROCESSING -------
            jedis.lrem(PROCESSING_QUEUE, 1, jobId);


            // -------- Release lock safely --------
            safeUnlock(jedis, lockKey, lockValue);

        } catch (Exception e) {
            log.error("Job {} failed: {}", jobId, e.getMessage());

            try (Jedis jedis = jedisPool.getResource()) {
                safeUnlock(jedis, lockKey, lockValue);
            }

            handleFailure(jobId);
        }
    }

    private void safeUnlock(Jedis jedis, String lockKey, String lockValue) {
        String currentValue = jedis.get(lockKey);

        if (lockValue.equals(currentValue)) {
            jedis.del(lockKey);
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
                // Move job to DLQ
                log.error("Job {} failed 3 times. Moving to DLQ!", jobId);
                jedis.hset(jobKey(jobId), "status", JobStatus.DLQ.name());
                jedis.rpush(DEAD_LETTER_QUEUE, jobId);

            } else {

                long delay = (attempts == 1) ? 10000 : 30000;
                long retryTime = System.currentTimeMillis() + delay;

                // Move job to RETRY_QUEUE
                log.warn("Job {} failed. Attempt {}. Will retry in {} seconds.", jobId, attempts, delay / 1000);
                jedis.hset(jobKey(jobId), "status", JobStatus.QUEUED.name());
                jedis.zadd(RETRY_QUEUE, retryTime, jobId);
            }
        }
    }
}
