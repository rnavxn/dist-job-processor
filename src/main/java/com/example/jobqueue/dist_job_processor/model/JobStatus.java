package com.example.jobqueue.dist_job_processor.model;

/**
 * Represents the lifecycle state of a job inside the system.
 *
 * QUEUED      → Job waiting in Redis queue
 * PROCESSING  → Worker currently executing the job
 * COMPLETED   → Job finished successfully
 * DLQ         → Job permanently failed
*/

public enum JobStatus {
    QUEUED,
    PROCESSING,
    COMPLETED,
    DLQ
}
