package com.example.jobqueue.dist_job_processor.DTO;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WebhookPayload {
    private String jobId;
    private String payload;
}