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

    public String enqueue(JobType type, String payload) {

        // Create job object with generated ID and timestamps
        Job job = new Job(type, payload);

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

            jedis.sadd(RedisKeys.ALL_JOBS_SET, job.getId());

            log.info("Job " + job.getId() + " enqueued successfully");

        } catch (Exception e) {
            System.err.println("Failed to enqueue job: " + e.getMessage());
        }

        return "Job " + job.getId() + " enqueued successfully!";
    }
}
