package com.example.jobqueue.dist_job_processor.model;

/**
 * Represents the lifecycle state of a job inside the system.
 *
 * QUEUED      → Job waiting in Redis queue
 * PROCESSING  → Worker currently executing the job
 * COMPLETED   → Job finished successfully
 * FAILED      → Temporarily failed job and will be moved to DLQ
 * DLQ         → Job permanently failed
*/

public enum JobStatus {
    QUEUED,
    PROCESSING,
    COMPLETED,
    FAILED,
    DLQ
}
