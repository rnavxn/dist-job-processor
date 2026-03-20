package com.example.jobqueue.dist_job_processor.controller;

import com.example.jobqueue.dist_job_processor.DTO.JobResponse;
import com.example.jobqueue.dist_job_processor.model.JobStatus;
import com.example.jobqueue.dist_job_processor.model.JobType;
import com.example.jobqueue.dist_job_processor.service.JobService;
import com.example.jobqueue.dist_job_processor.service.ProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Profile("producer")
@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobController {

    private final ProducerService producerService;
    private final JobService jobService;

    @PostMapping("/enqueue")
    public String createJob(@RequestParam JobType type, @RequestParam String payload) {
        // Push it to Redis
        return producerService.enqueue(type, payload);
    }

    @GetMapping("/{id}")
    public ResponseEntity<JobResponse> getJob(@PathVariable String id) {
        // Get the job from redis
        JobResponse job = jobService.getJob(id);

        if (job == null) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok(job);
    }

    @GetMapping
    public List<JobResponse> getAllJobs(@RequestParam(defaultValue = "10") int limit) {
        // Get all jobs from redis
        return jobService.getAllJobs(limit);
    }

    @GetMapping("/status/{status}")
    public List<JobResponse> getJobsByStatus(@PathVariable JobStatus status) {

        return jobService.getJobByStatus(status);
    }
}
