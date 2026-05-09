package com.example.jobqueue.dist_job_processor.DTO;

import com.example.jobqueue.dist_job_processor.model.JobType;
import lombok.Data;

@Data
public class JobRequest {
    private JobType type;
    private String payload;
    private String idempotencyKey;
    private String callbackUrl;
}