package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.model.Job;
import com.example.jobqueue.dist_job_processor.model.JobStatus;
import com.example.jobqueue.dist_job_processor.model.JobType;
import com.example.jobqueue.dist_job_processor.redis.RedisKeys;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;

@Profile("producer")
@Service
@RequiredArgsConstructor
@Slf4j
public class ProducerService {

    private final JedisPool jedisPool;

    private final JobPersistenceService persistenceService;

    public String enqueue(JobType type, String payload) {

        // Create job object with generated ID and timestamps
        Job job = new Job(type, payload);

        // ========== STEP 1: Save to PostgreSQL ==========
        // Save job with QUEUED status to database
        // If this fails, we throw exception and never touch Redis
        try {
            persistenceService.saveJob(job, JobStatus.QUEUED);
            log.info("Job {} saved to PostgreSQL", job.getId());
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

            // Add to set of all jobs (for dashboard queries)
            jedis.sadd(RedisKeys.ALL_JOBS_SET, job.getId());

            log.info("Job " + job.getId() + " enqueued successfully");

        } catch (Exception e) {
            // CRITICAL: If Redis fails, PostgreSQL already has the job
            // But we need to handle this - the job is in DB but not in queue
            // We'll handle this with a recovery mechanism later
            log.error("Failed to enqueue job to Redis after DB save: {}", e.getMessage());

            throw new RuntimeException("Redis unavailable", e);
        }

        return "Job " + job.getId() + " enqueued successfully!";
    }
}
