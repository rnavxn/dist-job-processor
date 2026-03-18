package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.DTO.JobResponse;
import com.example.jobqueue.dist_job_processor.model.JobStatus;
import com.example.jobqueue.dist_job_processor.model.JobType;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Map;

@Service
@RequiredArgsConstructor
public class JobService {

    private final JedisPool jedisPool;
    private static final String JOB_KEY_PREFIX = "job:";

    public JobResponse getJob(String jobId) {

        try (Jedis jedis = jedisPool.getResource()) {

            Map<String, String> data =
                    jedis.hgetAll(JOB_KEY_PREFIX + jobId);

            if (data == null || data.isEmpty()) {
                return null;
            }

            return new JobResponse(
                    data.get("id"),
                    JobType.valueOf(data.get("type")),
                    data.get("payload"),
                    JobStatus.valueOf(data.get("status")),
                    Integer.parseInt(data.get("attempts")),
                    Long.parseLong(data.get("createdAt")),
                    data.get("startedAt") != null
                            ? Long.parseLong(data.get("startedAt"))
                            : null
            );
        }
    }
}