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

    Optional<JobEntity> findById(String id);

    List<JobEntity> findByStatus(JobStatus status);

    @Modifying
    @Transactional
    @Query("UPDATE JobEntity j SET j.status = :status, j.startedAt = :startedAt WHERE j.id = :id")
    void updateStatusAndStartedAt(@Param("id") String id,
                                  @Param("status") JobStatus status,
                                  @Param("startedAt") Long startedAt);

    @Modifying
    @Transactional
    @Query("UPDATE JobEntity j SET j.status = :status, j.completedAt = :completedAt WHERE j.id = :id")
    void markCompleted(@Param("id") String id,
                       @Param("status") JobStatus status,
                       @Param("completedAt") Long completedAt);

    @Modifying
    @Transactional
    @Query("UPDATE JobEntity j SET j.status = :status, j.attempts = j.attempts + 1, " +
            "j.lastError = :error WHERE j.id = :id")
    void incrementAttemptsAndSetError(@Param("id") String id,
                                      @Param("status") JobStatus status,
                                      @Param("error") String error);

    @Modifying
    @Transactional
    @Query("UPDATE JobEntity j SET j.lastError = :error WHERE j.id = :id")
    void updateLastError(@Param("id") String id,
                         @Param("error") String error);

}