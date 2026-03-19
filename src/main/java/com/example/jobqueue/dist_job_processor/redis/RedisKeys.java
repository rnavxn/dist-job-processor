package com.example.jobqueue.dist_job_processor.redis;

public class RedisKeys {

    public static final String JOB_QUEUE = "job_queue";
    public static final String PROCESSING_QUEUE = "processing_queue";
    public static final String RETRY_QUEUE = "retry_queue";
    public static final String DEAD_LETTER_QUEUE = "dead_letter_queue";

    public static final String JOB_PREFIX = "job:";
    public static final String ALL_JOBS_SET = "jobs:all";

    public static String jobKey(String jobId) {
        return JOB_PREFIX + jobId;
    }

    public static String lockKey(String jobId) {
        return JOB_PREFIX + jobId + ":lock";
    }
}