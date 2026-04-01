package com.example.jobqueue.dist_job_processor.config;

public class JobConstants {

    // Redis TTL (Time To Live) in seconds
    public static final int COMPLETED_JOB_TTL_SECONDS = 3600;      // 1 hour
    public static final int DLQ_JOB_TTL_SECONDS = 604800;          // 7 days
    public static final int LOCK_TTL_SECONDS = 30;                  // 30 seconds

    // Timeouts in milliseconds
    public static final long STUCK_JOB_TIMEOUT_MS = 60000;          // 1 minute
    public static final long QUEUE_STUCK_TIMEOUT_MS = 30000;        // 30 seconds

    // Retry Configuration
    public static final long RETRY_BASE_DELAY_MS = 10000;           // 10 seconds
    public static final long RETRY_MAX_DELAY_MS = 40000;            // 40 seconds

    // Scheduler intervals in milliseconds
    public static final long REAPER_INTERVAL_MS = 15000;            // 15 seconds
    public static final long RECONCILIATION_INTERVAL_MS = 30000;    // 30 seconds
    public static final long RETRY_SERVICE_INTERVAL_MS = 5000;      // 5 seconds

    // Batch sizes
    public static final int RECONCILIATION_BATCH_SIZE = 50;
    public static final int REAPER_BATCH_SIZE = 50;
    public static final int RETRY_BATCH_SIZE = 50;

    // Worker configuration
    public static final int WORKER_COUNT = 2;
    public static final int SIMULATED_TASK_DURATION_MS = 2000;      // For testing
    public static final double SIMULATED_FAILURE_RATE = 0.2;        // 20% for testing

    // Lock retry configuration
    public static final long LOCK_RETRY_BASE_DELAY_MS = 2000;       // 2 seconds
    public static final long LOCK_RETRY_MAX_JITTER_MS = 3000;       // +3 seconds max
}