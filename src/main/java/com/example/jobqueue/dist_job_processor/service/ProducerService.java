package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.entity.JobEntity;
import com.example.jobqueue.dist_job_processor.metrics.JobMetrics;
import com.example.jobqueue.dist_job_processor.model.Job;
import com.example.jobqueue.dist_job_processor.model.JobStatus;
import com.example.jobqueue.dist_job_processor.model.JobType;
import com.example.jobqueue.dist_job_processor.redis.RedisKeys;
import com.example.jobqueue.dist_job_processor.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.util.DigestUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Profile("producer")
@Service
@RequiredArgsConstructor
@Slf4j
public class ProducerService {

    private final JedisPool jedisPool;
    private final JobRepository jobRepository;
    private final JobPersistenceService persistenceService;
    private final JobMetrics jobMetrics;

    public String enqueue(JobType type, String payload, String idempotencyKey) {

        // Generate key from payload hash if caller didn't provide one
        if (idempotencyKey == null) {
            idempotencyKey = generateHash(type, payload);
        }

        // Check if job with this key already exists
        Optional<JobEntity> existing = jobRepository.findByIdempotencyKey(idempotencyKey);

        if (existing.isPresent()) {
            log.info("Duplicate submission detected for key {}", idempotencyKey);
            return "Job already exists: " + existing.get().getId();
        }

        // Create job object with generated ID and timestamps
        Job job = new Job(type, payload);
        job.setIdempotencyKey(idempotencyKey);

        // ========== STEP 1: Save to PostgreSQL ==========
        try {
            persistenceService.saveJob(job, JobStatus.QUEUED);
            log.info("Job {} saved to PostgreSQL", job.getId());

        } catch (DataIntegrityViolationException e) {
            // Two requests hit simultaneously with same key
            // DB unique constraint saved us — return existing job
            log.warn("Idempotency key collision for key {}", idempotencyKey);
            return jobRepository.findByIdempotencyKey(idempotencyKey)
                    .map(jobEntity -> "Job already exists: " + jobEntity.getId())
                    .orElse("Duplicate rejected");

        } catch (Exception e) {
            log.error("Failed to save job to PostgreSQL: {}", e.getMessage());
            throw new RuntimeException("Database unavailable", e);
        }

        // ========== STEP 2: Save to Redis ==========
        try (Jedis jedis = jedisPool.getResource()) {
            // Redis key used to store job metadata
            String jobKey = RedisKeys.jobKey(job.getId());

            // Store job fields as Redis hash for easy updates
            Map<String, String> jobData = new HashMap<>();
            jobData.put("id", job.getId());
            jobData.put("type", job.getType().name());
            jobData.put("payload", job.getPayload());
            jobData.put("createdAt", String.valueOf(job.getCreatedAt()));
            jobData.put("attempts", String.valueOf(job.getAttempts()));
            jobData.put("status", JobStatus.QUEUED.name());

            // Persist job metadata
            jedis.hset(jobKey, jobData);

            // Push job ID into Redis queue for workers
            jedis.rpush(RedisKeys.JOB_QUEUE, job.getId());

            log.info("Job " + job.getId() + " enqueued successfully");

            // Update monitor
            jobMetrics.getJobsEnqueued().increment();

        } catch (Exception e) {
            // NOTE: If Redis fails, PostgreSQL already has the job. Reconciliation service will add job later
            log.error("Failed to enqueue job to Redis after DB save: {}", e.getMessage());

            throw new RuntimeException("Redis unavailable", e);
        }

        return "Job " + job.getId() + " enqueued successfully!";
    }

    private String generateHash(JobType type, String payload) {
        String input = type.name() + ":" + payload;
        return DigestUtils.md5DigestAsHex(input.getBytes());
    }
}
