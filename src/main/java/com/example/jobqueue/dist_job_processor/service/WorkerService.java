package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.model.Job;
import com.example.jobqueue.dist_job_processor.model.JobStatus;
import com.example.jobqueue.dist_job_processor.model.JobType;
import com.example.jobqueue.dist_job_processor.redis.RedisKeys;
import com.example.jobqueue.dist_job_processor.redis.RedisScriptManager;
import com.example.jobqueue.dist_job_processor.redis.RedisUtils;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.args.ListDirection;

import java.util.List;
import java.util.Map;


@Profile("worker")
@Service
@RequiredArgsConstructor
@Slf4j // This gives us a 'log' object instead of using System.out
public class WorkerService {

    private final JedisPool jedisPool;

    private final RedisScriptManager scriptManager;

    @PostConstruct
    public void startWorkers() {

        int workerCount = 2;

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
                        RedisKeys.JOB_QUEUE,
                        RedisKeys.PROCESSING_QUEUE,
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

        String lockKey = RedisKeys.jobKey(jobId) + ":lock";
        String lockValue = java.util.UUID.randomUUID().toString();

        try (Jedis jedis = jedisPool.getResource()){

            // -------- Initial status check --------
            String status = jedis.hget(RedisKeys.jobKey(jobId), "status");
            if ("COMPLETED".equals(status)) {
                log.warn("Skipping already completed job {}", jobId);
                jedis.lrem(RedisKeys.PROCESSING_QUEUE, 1, jobId);
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
                jedis.lrem(RedisKeys.PROCESSING_QUEUE, 1, jobId);
                return;
            }

            // -------- Double-check after lock --------
            status = jedis.hget(RedisKeys.jobKey(jobId), "status");
            if ("COMPLETED".equals(status)) {
                jedis.lrem(RedisKeys.PROCESSING_QUEUE, 1, jobId);
                RedisUtils.safeUnlock(jedis, lockKey, lockValue);
                return;
            }

            // -------- Load job --------
            Map<String, String> jobData = jedis.hgetAll(RedisKeys.jobKey((jobId)));

            if (jobData == null || jobData.isEmpty()) {                 // If Redis loses metadata (rare but possible)
                log.error("Job metadata missing for {}", jobId);
                jedis.lrem(RedisKeys.PROCESSING_QUEUE, 1, jobId);
                RedisUtils.safeUnlock(jedis, lockKey, lockValue);
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
            jedis.hset(RedisKeys.jobKey(jobId), "status", JobStatus.PROCESSING.name());
            jedis.hset(RedisKeys.jobKey(jobId), "startedAt", String.valueOf(job.getStartedAt()));

            // ------- Task Execution -------
            Thread.sleep(3000); // Simulate a 3-second task attempt

            // ------- A RANDOM FAILURE -------
            // Let's say 20% of jobs fail randomly to test our logic
            if (Math.random() < 0.2)
                throw new RuntimeException("Simulated Network Error");

            Thread.sleep(7000);

            // Mark COMPLETED
            jedis.hset(RedisKeys.jobKey(jobId), "status", JobStatus.COMPLETED.name());
            log.info("Finished: {}", job.getId());

            // ------- Remove from PROCESSING -------
            jedis.lrem(RedisKeys.PROCESSING_QUEUE, 1, jobId);


            // -------- Release lock safely --------
            RedisUtils.safeUnlock(jedis, lockKey, lockValue);

        } catch (Exception e) {
            log.error("Job {} failed: {}", jobId, e.getMessage());

            try (Jedis jedis = jedisPool.getResource()) {
                RedisUtils.safeUnlock(jedis, lockKey, lockValue);
            }

            handleFailure(jobId);
        }
    }


    private void handleFailure(String jobId) {

        try (Jedis jedis = jedisPool.getResource()) {
            // increment attempts counter
            long attempts = jedis.hincrBy(RedisKeys.jobKey(jobId), "attempts", 1);

            if (attempts >= 3) {
                // Move job to DLQ

                // Atomic move using manual Lua script
                Object ob = jedis.eval(
                        scriptManager.get("fail_to_dlq"),
                        List.of(RedisKeys.PROCESSING_QUEUE, RedisKeys.DEAD_LETTER_QUEUE),
                        List.of(jobId)
                );

                if ((Long) ob == 1L) {
                    log.error("Job {} failed 3 times. Moving to DLQ!", jobId);
                    jedis.hset(RedisKeys.jobKey(jobId), "status", JobStatus.DLQ.name());
                }

            } else {
                // Move job to RETRY_QUEUE
                long delay = (attempts == 1) ? 10000 : 30000;
                long retryTime = System.currentTimeMillis() + delay;

                // Atomic move using manual Lua script
                Object ob = jedis.eval(
                        scriptManager.get("fail_to_retry"),
                        List.of(RedisKeys.PROCESSING_QUEUE, RedisKeys.RETRY_QUEUE),
                        List.of(jobId, String.valueOf(retryTime))
                );

                if ((Long) ob == 1L) {
                    log.warn("Job {} failed. Attempt {}. Will retry in {} seconds.", jobId, attempts, delay / 1000);
                    jedis.hset(RedisKeys.jobKey(jobId), "status", JobStatus.QUEUED.name());
                }
            }
        }
    }

}
