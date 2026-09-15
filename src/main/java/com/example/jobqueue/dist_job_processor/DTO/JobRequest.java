package com.example.jobqueue.dist_job_processor.DTO;

import com.example.jobqueue.dist_job_processor.model.JobType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class JobRequest {
    @NotNull(message = "Job type is required")
    private JobType type;
    @NotBlank(message = "Payload is required")
    private String payload;
    private String idempotencyKey;
    private String callbackUrl;
}