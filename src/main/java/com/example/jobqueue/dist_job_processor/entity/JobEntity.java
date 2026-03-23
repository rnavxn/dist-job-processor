package com.example.jobqueue.dist_job_processor.entity;

import com.example.jobqueue.dist_job_processor.model.JobStatus;
import com.example.jobqueue.dist_job_processor.model.JobType;
import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "jobs")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class JobEntity {

    @Id
    private String id;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private JobType type;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private JobStatus status;

    @Column(nullable = false)
    private Integer attempts = 0;

    @Column(name = "created_at", nullable = false)
    private Long createdAt;

    @Column(name = "started_at")
    private Long startedAt;

    @Column(name = "completed_at")
    private Long completedAt;

    @Column(name = "last_error", columnDefinition = "TEXT")
    private String lastError;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    // Convenience constructor from existing Job
    public JobEntity(String id, JobType type, String payload, JobStatus status,
                     int attempts, long createdAt, Long startedAt) {
        this.id = id;
        this.type = type;
        this.payload = payload;
        this.status = status;
        this.attempts = attempts;
        this.createdAt = createdAt;
        this.startedAt = startedAt;
        this.updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    @PrePersist
    protected void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }
}
