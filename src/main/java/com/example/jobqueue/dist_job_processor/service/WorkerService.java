package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.DTO.WebhookPayload;
import com.example.jobqueue.dist_job_processor.config.JobConstants;
import com.example.jobqueue.dist_job_processor.metrics.JobMetrics;
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
import org.springframework.web.client.RestTemplate;
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
    private final JobPersistenceService persistenceService;
    private final JobMetrics jobMetrics;
    private final RestTemplate restTemplate;

    @PostConstruct
    public void startWorkers() {
        int workerCount = JobConstants.WORKER_COUNT;

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

        String lockKey = RedisKeys.lockKey(jobId);
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
                    redis.clients.jedis.params.SetParams.setParams().nx().ex(JobConstants.LOCK_TTL_SECONDS)
            );

            if (lockResult == null) {
                log.warn("Job {} already locked, skipping", jobId);

                long delay = JobConstants.LOCK_RETRY_BASE_DELAY_MS + (long)(Math.random() * JobConstants.LOCK_RETRY_MAX_JITTER_MS);
                long retryTime = System.currentTimeMillis() + delay;

                Object ob = jedis.eval(
                        scriptManager.get("processing_to_retry"),
                        List.of(RedisKeys.PROCESSING_QUEUE, RedisKeys.RETRY_QUEUE),
                        List.of(jobId, String.valueOf(retryTime))
                );

                if ((Long) ob == 1L) {
                    log.error("Job {} moved to Retry Queue successfully!", jobId);
                    jedis.hset(RedisKeys.jobKey(jobId), "status", JobStatus.QUEUED.name());
                }
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

            // Set CallbackUrl in worker-side job re-construction
            job.setCallbackUrl(jobData.get("callbackUrl"));

            log.info("Thread {} starting Job: {}", Thread.currentThread().getName(), job.getId());

            // ========== Update PostgreSQL to PROCESSING ==========
            // Why? So database knows this job is being worked on.
            // If worker crashes now, Reaper will see stuck job and recover it.
            long startedAt = System.currentTimeMillis();
            persistenceService.updateStatusToProcessing(jobId, startedAt);

            // ------- Mark Processing in Redis -------
            job.setStartedAt(startedAt);
            jedis.hset(RedisKeys.jobKey(jobId), "status", JobStatus.PROCESSING.name());
            jedis.hset(RedisKeys.jobKey(jobId), "startedAt", String.valueOf(job.getStartedAt()));

            // ------- Task Execution -------
            try {
                jobMetrics.getJobProcessingTime().record(() -> {
                    // NEW WEBHOOK LOGIC
                    if (job.getCallbackUrl() != null && !job.getCallbackUrl().isBlank()) {
                        log.info("Executing Webhook for Job {} to {}", job.getId(), job.getCallbackUrl());

                        WebhookPayload requestBody = new WebhookPayload(jobId, job.getPayload());

                        org.springframework.http.ResponseEntity<String> response =
                                restTemplate.postForEntity(job.getCallbackUrl(), requestBody, String.class);

                        if (!response.getStatusCode().is2xxSuccessful()) {
                            throw new RuntimeException("Webhook failed with status: " + response.getStatusCode());
                        }
                    } else {
                        // BACKWARD COMPATIBILITY: Simulated task if no webhook provided
                        try {
                            long processingTime = JobConstants.SIMULATED_TASK_MIN_MS +
                                    (long)(Math.random() * (JobConstants.SIMULATED_TASK_MAX_MS - JobConstants.SIMULATED_TASK_MIN_MS));
                            Thread.sleep(processingTime);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }

                        // ------- A RANDOM FAILURE -------
                        // Let's say 20% of jobs fail randomly to test our logic
                        if (Math.random() < JobConstants.SIMULATED_FAILURE_RATE) {
                            throw new RuntimeException("Simulated Network Error");
                        }
                    }
                });
            } catch (Exception e) {
                // rethrow so the outer catch handles failure logic
                throw e;
            }

            // ========== Mark COMPLETED in PostgreSQL ==========
            // Success path: job finished without errors
            persistenceService.markCompleted(jobId);

            // Mark COMPLETED in Redis
            jedis.hset(RedisKeys.jobKey(jobId), "status", JobStatus.COMPLETED.name());
            log.info("Finished: {}", job.getId());

            jobMetrics.getJobsCompleted().increment();

            // ------- Remove from PROCESSING -------
            jedis.lrem(RedisKeys.PROCESSING_QUEUE, 1, jobId);

            // -------- Release lock safely --------
            RedisUtils.safeUnlock(jedis, lockKey, lockValue);

            // Automated job cleanup
            // Remove completed job metadata from Redis after 1 hour
            // But keep in PostgreSQL forever
            jedis.expire(RedisKeys.jobKey(jobId), JobConstants.COMPLETED_JOB_TTL_SECONDS);

        } catch (Exception e) {
            log.error("Job {} failed: {}", jobId, e.getMessage());

            try (Jedis jedis = jedisPool.getResource()) {
                RedisUtils.safeUnlock(jedis, lockKey, lockValue);
            }

            jobMetrics.getJobsFailed().increment();

            handleFailure(jobId, e.getMessage());
        }
    }


    private void handleFailure(String jobId, String errorMessage) {

        try (Jedis jedis = jedisPool.getResource()) {
            // increment attempts counter in Redis
            long attempts = jedis.hincrBy(RedisKeys.jobKey(jobId), "attempts", 1);

            if (attempts >= 4) {
                // ========== Move to DLQ in PostgreSQL ==========
                // Job failed 4 times. It's dead. Store in DLQ for manual inspection.
                persistenceService.moveToDlq(jobId, errorMessage);

                // Move job to DLQ in Redis
                Object ob = jedis.eval(
                        scriptManager.get("processing_to_dlq"),
                        List.of(RedisKeys.PROCESSING_QUEUE, RedisKeys.DEAD_LETTER_QUEUE),
                        List.of(jobId)
                );

                // Update metrics
                jobMetrics.getJobsMovedToDlq().increment();

                if ((Long) ob == 1L) {
                    log.error("Job {} failed 4 times. Moving to DLQ!", jobId);
                    jedis.hset(RedisKeys.jobKey(jobId), "status", JobStatus.DLQ.name());

                    jedis.expire(RedisKeys.jobKey(jobId), JobConstants.DLQ_JOB_TTL_SECONDS);
                }

            } else {
                // ========== Update PostgreSQL for retry ==========
                // Job failed but has attempts left. Increment attempts and set status back to QUEUED.
                persistenceService.incrementAttemptsAndMoveToQueued(jobId, errorMessage);

                // Move job to RETRY_QUEUE in Redis
                long baseDelay = JobConstants.RETRY_BASE_DELAY_MS;
                long delay = (long) (baseDelay * Math.pow(2, attempts - 1));

                long maxDelay = JobConstants.RETRY_MAX_DELAY_MS;
                delay = Math.min(delay, maxDelay);
                long retryTime = System.currentTimeMillis() + delay;

                Object ob = jedis.eval(
                        scriptManager.get("processing_to_retry"),
                        List.of(RedisKeys.PROCESSING_QUEUE, RedisKeys.RETRY_QUEUE),
                        List.of(jobId, String.valueOf(retryTime))
                );

                if ((Long) ob == 1L) {
                    log.warn("Job {} failed. Attempt {}. Will retry in {} seconds.", jobId, attempts, delay / 1000);
                    jedis.hset(RedisKeys.jobKey(jobId), "status", JobStatus.QUEUED.name());
                }
            }
        } catch (Exception e) {
            // NOTE: If PostgreSQL update fails, reconciliation service will fix inconsistency
            log.error("CRITICAL: Failed to update PostgreSQL for job {}: {}", jobId, e.getMessage());
        }
    }

}
