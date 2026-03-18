package com.example.jobqueue.dist_job_processor.DTO;

import com.example.jobqueue.dist_job_processor.model.JobStatus;
import com.example.jobqueue.dist_job_processor.model.JobType;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class JobResponse {

    private String id;
    private JobType type;
    private String payload;
    private JobStatus status;
    private int attempts;
    private long createdAt;
    private Long startedAt;
}