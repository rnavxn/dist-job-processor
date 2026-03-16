package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.model.Job;
import com.example.jobqueue.dist_job_processor.model.JobStatus;
import com.example.jobqueue.dist_job_processor.model.JobType;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class ProducerService {

    private final JedisPool jedisPool;

    private static final String JOB_QUEUE = "job_queue";
    private static final String JOB_KEY_PREFIX = "job:";

    public String enqueue(JobType type, String payload) {

        // Create job object with generated ID and timestamps
        Job job = new Job(type, payload);

        try (Jedis jedis = jedisPool.getResource()) {

            // Redis key used to store job metadata
            String jobKey = JOB_KEY_PREFIX + job.getId();

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
            jedis.rpush(JOB_QUEUE, job.getId());

        } catch (Exception e) {
            System.err.println("Failed to enqueue job: " + e.getMessage());
        }

        return "Job " + job.getId() + " enqueued successfully!";
    }
}
