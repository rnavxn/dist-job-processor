package com.example.jobqueue.dist_job_processor.controller;

import com.example.jobqueue.dist_job_processor.model.Job;
import com.example.jobqueue.dist_job_processor.service.ProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobController {

    private final ProducerService producerService;

    @PostMapping("/enqueue")
    public String createJob(@RequestParam String type, @RequestParam String payload) {
        // Create a new Job object
        Job job = new Job();
        job.setType(type);
        job.setPayload(payload);

        // Push it to Redis
        producerService.enqueue(job);

        return "Job " + job.getId() + " enqueued successfully!";
    }
}
