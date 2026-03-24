package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.entity.JobEntity;
import com.example.jobqueue.dist_job_processor.model.Job;
import com.example.jobqueue.dist_job_processor.model.JobStatus;
import com.example.jobqueue.dist_job_processor.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/**
 * Bridge between our application and PostgreSQL.
 *
 * This service handles ALL database operations for jobs.
 * Why separate? So if we ever change from PostgreSQL to another database,
 * we only change this one file, not every service.
 *
 * @Transactional means: if anything fails inside, ALL changes roll back.
 * Example: If we're updating 3 fields and the 2nd one fails, the 1st one undoes.
 */

@Service
@RequiredArgsConstructor
@Slf4j
public class JobPersistenceService {

    // Spring Data JPA handles actual SQL. We just call methods like .save(), .findById()
    private final JobRepository jobRepository;

    /**
     * Saves a NEW job to database.
     * Used when: Producer receives a job request.
     *
     * @param job The job object from our code
     * @param status Usually QUEUED for new jobs
     */
    @Transactional
    public void saveJob(Job job, JobStatus status) {
        // Convert our simple Job class to JPA Entity (has database annotations)
        JobEntity entity = new JobEntity(
                job.getId(),
                job.getType(),
                job.getPayload(),
                status,                 // ← This is QUEUED
                job.getAttempts(),
                job.getCreatedAt(),
                job.getStartedAt()      // null for new jobs
        );

        // Spring generates INSERT SQL automatically
        jobRepository.save(entity);

        log.debug("Saved job {} to PostgreSQL with status {}", job.getId(), status);
    }

    /**
     * Updates job when a worker starts processing it.
     * Used when: Worker picks job from queue.
     *
     * Two changes:
     * 1. status: QUEUED → PROCESSING
     * 2. startedAt: null → current timestamp
     */
    @Transactional
    public void updateStatusToProcessing(String jobId, Long startedAt) {
        jobRepository.updateStatusAndStartedAt(jobId, JobStatus.PROCESSING, startedAt);
        log.debug("Updated job {} to PROCESSING in PostgreSQL", jobId);
    }


    /**
     * Marks job as successfully completed.
     * Used when: Worker finishes without error.
     *
     * Two changes:
     * 1. status: PROCESSING → COMPLETED
     * 2. completedAt: null → current timestamp
     */
    @Transactional
    public void markCompleted(String jobId) {
        jobRepository.markCompleted(jobId, JobStatus.COMPLETED, System.currentTimeMillis());
        log.debug("Marked job {} as COMPLETED in PostgreSQL", jobId);
    }

    /**
     * Handles a failed job that will be retried.
     * Used when: Job fails but has attempts left (attempts < 3).
     *
     * Three changes:
     * 1. attempts: attempts + 1
     * 2. status: PROCESSING → QUEUED (back to queue)
     * 3. last_error: stores error message for debugging
     */
    @Transactional
    public void incrementAttemptsAndMoveToQueued(String jobId, String errorMessage) {
        jobRepository.incrementAttemptsAndSetError(jobId, JobStatus.QUEUED, errorMessage);
        log.debug("Incremented attempts for job {} and set status to QUEUED", jobId);
    }

    /**
     * Handles a job that failed permanently.
     * Used when: Job fails and attempts >= 3.
     *
     * Two changes:
     * 1. status: PROCESSING → DLQ (Dead Letter Queue)
     * 2. last_error: stores final error message
     *
     * These jobs need manual inspection later.
     */
    @Transactional
    public void moveToDlq(String jobId, String errorMessage) {
        jobRepository.incrementAttemptsAndSetError(jobId, JobStatus.DLQ, errorMessage);
        log.warn("Job {} moved to DLQ in PostgreSQL after max attempts", jobId);
    }

    /**
     * Fetches a job from database by ID.
     * Used when: Dashboard needs to show job details.
     *
     * @return Optional (might be empty if job doesn't exist)
     */
    public Optional<JobEntity> findJob(String jobId) {
        return jobRepository.findById(jobId);
    }

    /**
     * Record that a retry is happening for this job.
     * We don't change status (should already be QUEUED), but we track the retry.
     *
     * Used when: RetryService moves job from retry_queue to job_queue.
     */
    @Transactional
    public void recordRetry(String jobId) {
        // Option 1: Add a retry_count column to track retries separately
        // Option 2: Update last_error with retry timestamp
        // Option 3: Add to a retry_history table

        // For now, let's just log and update last_error
        jobRepository.updateLastError(jobId, "Retried at: " + System.currentTimeMillis());
        log.debug("Recorded retry for job {}", jobId);
    }
}