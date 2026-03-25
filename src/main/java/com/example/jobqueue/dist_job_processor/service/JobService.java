package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.DTO.JobResponse;
import com.example.jobqueue.dist_job_processor.entity.JobEntity;
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
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class JobService {

    // CHANGED: Now using PostgreSQL instead of Redis
    private final JobPersistenceService persistenceService;

    /**
     * Get single job by ID from PostgreSQL
     */
    public JobResponse getJob(String jobId) {
        return persistenceService.findJob(jobId)
                .map(this::convertToResponse)
                .orElse(null);
    }

    /**
     * Get all jobs (limited) from PostgreSQL, ordered by most recent
     */
    public List<JobResponse> getAllJobs(int limit) {
        return persistenceService.findAllJobs(limit)
                .stream()
                .map(this::convertToResponse)
                .collect(Collectors.toList());
    }

    /**
     * Get jobs filtered by status from PostgreSQL
     */
    public List<JobResponse> getJobsByStatus(JobStatus status) {
        return persistenceService.findByStatus(status)
                .stream()
                .map(this::convertToResponse)
                .collect(Collectors.toList());
    }

    /**
     * Convert JPA Entity to DTO for API response
     */
    private JobResponse convertToResponse(JobEntity entity) {
        return new JobResponse(
                entity.getId(),
                entity.getType(),
                entity.getPayload(),
                entity.getStatus(),
                entity.getAttempts(),
                entity.getCreatedAt(),
                entity.getStartedAt()
        );
    }
}