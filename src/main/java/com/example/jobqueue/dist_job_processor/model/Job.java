package com.example.jobqueue.dist_job_processor.model;

import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Job {
    private String id = UUID.randomUUID().toString();
    private String type;        // e.g., "EMAIL_SEND", "IMAGE_RESIZE"
    private String payload;     // The actual data (JSON string or simple text)
    private long createdAt = System.currentTimeMillis();
    private long startedAt;
    private int attempts = 0;   // to track how many times a job has failed
}
