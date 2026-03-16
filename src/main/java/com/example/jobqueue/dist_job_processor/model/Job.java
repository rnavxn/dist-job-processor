package com.example.jobqueue.dist_job_processor.model;

import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Job {
    private String id;
    private JobType type;
    private String payload;
    private long createdAt;
    private Long startedAt;
    private int attempts = 0;

    public Job(JobType type, String payload) {
        this.id = UUID.randomUUID().toString();
        this.type = type;
        this.payload = payload;
        this.createdAt = System.currentTimeMillis();
        this.attempts = 0;
    }
}
