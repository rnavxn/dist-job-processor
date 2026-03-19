package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.DTO.JobResponse;
import com.example.jobqueue.dist_job_processor.model.JobStatus;
import com.example.jobqueue.dist_job_processor.model.JobType;
import com.example.jobqueue.dist_job_processor.redis.RedisKeys;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Service
@RequiredArgsConstructor
public class JobService {

    private final JedisPool jedisPool;

    public JobResponse getJob(String jobId) {

        try (Jedis jedis = jedisPool.getResource()) {

            Map<String, String> data =
                    jedis.hgetAll(RedisKeys.jobKey(jobId));

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

    public List<JobResponse> getAllJobs(int limit) {

        try (Jedis jedis = jedisPool.getResource()) {

            Set<String> jobIds = jedis.smembers(RedisKeys.ALL_JOBS_SET);

            return jobIds.stream()
                    .limit(limit)
                    .map(this::getJob)
                    .filter(Objects::nonNull)
                    .toList();
        }
    }

    public List<JobResponse> getJobByStatus(JobStatus status) {

        try (Jedis jedis = jedisPool.getResource()) {

            Set<String> jobIds = jedis.smembers(RedisKeys.ALL_JOBS_SET);

            return jobIds.stream()
                    .map(this::getJob)
                    .filter(job -> job != null && job.getStatus() == status)
                    .toList();
        }
    }
}