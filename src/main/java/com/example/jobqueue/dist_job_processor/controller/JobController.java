package com.example.jobqueue.dist_job_processor.controller;

import com.example.jobqueue.dist_job_processor.DTO.JobResponse;
import com.example.jobqueue.dist_job_processor.model.JobStatus;
import com.example.jobqueue.dist_job_processor.model.JobType;
import com.example.jobqueue.dist_job_processor.service.JobService;
import com.example.jobqueue.dist_job_processor.service.ProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.Objects;
import java.util.Set;

@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobController {

    private final ProducerService producerService;
    private final JobService jobService;
    private final JedisPool jedisPool;

    @PostMapping("/enqueue")
    public String createJob(@RequestParam JobType type, @RequestParam String payload) {
        // Push it to Redis
        return producerService.enqueue(type, payload);
    }

    @GetMapping("/{id}")
    public ResponseEntity<JobResponse> getJob(@PathVariable String id) {

        JobResponse job = jobService.getJob(id);

        if (job == null) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok(job);
    }

    @GetMapping
    public List<JobResponse> getAllJobs() {

        try (Jedis jedis = jedisPool.getResource()) {

            Set<String> jobIds = jedis.smembers("jobs:all");

            return jobIds.stream()
                    .map(jobService::getJob)
                    .filter(Objects::nonNull)
                    .toList();
        }
    }

    @GetMapping("/status/{status}")
    public List<JobResponse> getJobsByStatus(@PathVariable JobStatus status) {

        try (Jedis jedis = jedisPool.getResource()) {

            Set<String> jobIds = jedis.smembers("jobs:all");

            return jobIds.stream()
                    .map(jobService::getJob)
                    .filter(job -> job != null && job.getStatus() == status)
                    .toList();
        }
    }
}
