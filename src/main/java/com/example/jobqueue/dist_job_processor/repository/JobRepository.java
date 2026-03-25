package com.example.jobqueue.dist_job_processor.repository;

import com.example.jobqueue.dist_job_processor.entity.JobEntity;
import com.example.jobqueue.dist_job_processor.model.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Repository
public interface JobRepository extends JpaRepository<JobEntity, String> {

    /**
     * Find job by its unique ID
     * Used for: API lookups, status checks, recovery operations
     */
    Optional<JobEntity> findById(String id);

    /**
     * Find all jobs with a specific status
     * Used for: Dashboard filtering, batch operations, monitoring
     */
    List<JobEntity> findByStatus(JobStatus status);

    /**
     * Update job when a worker starts processing it
     * Changes: QUEUED → PROCESSING, sets startedAt timestamp
     * Used by: WorkerService.processTask()
     */
    @Modifying
    @Transactional
    @Query("UPDATE JobEntity j SET j.status = :status, j.startedAt = :startedAt WHERE j.id = :id")
    void updateStatusAndStartedAt(@Param("id") String id,
                                  @Param("status") JobStatus status,
                                  @Param("startedAt") Long startedAt);

    /**
     * Mark job as successfully completed
     * Changes: PROCESSING → COMPLETED, sets completedAt timestamp
     * Used by: WorkerService on successful execution
     */
    @Modifying
    @Transactional
    @Query("UPDATE JobEntity j SET j.status = :status, j.completedAt = :completedAt WHERE j.id = :id")
    void markCompleted(@Param("id") String id,
                       @Param("status") JobStatus status,
                       @Param("completedAt") Long completedAt);

    /**
     * Handle job failure with retry
     * Increments attempts counter and stores error message
     * Used by: WorkerService.handleFailure() when attempts < max
     */
    @Modifying
    @Transactional
    @Query("UPDATE JobEntity j SET j.status = :status, j.attempts = j.attempts + 1, " +
            "j.lastError = :error WHERE j.id = :id")
    void incrementAttemptsAndSetError(@Param("id") String id,
                                      @Param("status") JobStatus status,
                                      @Param("error") String error);

    /**
     * Record retry event without changing attempts count
     * Updates lastError with retry timestamp for debugging
     * Used by: RetryService when moving job back to main queue
     */
    @Modifying
    @Transactional
    @Query("UPDATE JobEntity j SET j.lastError = :error WHERE j.id = :id")
    void updateLastError(@Param("id") String id,
                         @Param("error") String error);

    /**
     * Recover a stuck job that was stuck in PROCESSING state.
     * Resets status to QUEUED and records recovery reason.
     */
    @Modifying
    @Transactional
    @Query("UPDATE JobEntity j SET j.status = :status, j.startedAt = NULL, j.lastError = :error WHERE j.id = :id")
    void recoverStuckJob(@Param("id") String id,
                         @Param("status") JobStatus status,
                         @Param("error") String error);

    /**
     * Get N most recent jobs, ordered by creation date descending
     * Used by: JobService.getAllJobs() for API listing
     */
    @Query("SELECT j FROM JobEntity j ORDER BY j.createdAt DESC LIMIT ?1")
    List<JobEntity> findTopNByOrderByCreatedAtDesc(int limit);
}