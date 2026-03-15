package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.model.Job;
import com.google.gson.Gson;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@Service
@RequiredArgsConstructor
public class ProducerService {

    private final JedisPool jedisPool;
    private final Gson gson = new Gson();
    private static final String QUEUE_NAME = "job_queue";

    public String enqueue(String type, String payload) {
        // Create a new Job object
        Job job = new Job();
        job.setType(type);
        job.setPayload(payload);

        String jsonJob = gson.toJson(job);

        try (Jedis jedis = jedisPool.getResource()) {
            // RPUSH adds the job to the end of the list
            jedis.rpush(QUEUE_NAME, jsonJob);
            System.out.println("Job enqueued: " + job.getId());
        } catch (Exception e) {
            // Handle connection or serialization errors here
            System.err.println("Failed to enqueue job: " + e.getMessage());
        }

        return "Job " + job.getId() + " enqueued successfully!";
    }
}
