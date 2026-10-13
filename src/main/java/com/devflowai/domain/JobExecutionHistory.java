package com.devflowai.domain;

import com.devflowai.job.JobStatus;
import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "job_execution_history", indexes = {
        @Index(name = "idx_job_exec_job_id", columnList = "job_id"),
        @Index(name = "idx_job_exec_status", columnList = "status")
})
public class JobExecutionHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "job_id", nullable = false)
    private JobEntity job;

    @Column(name = "status", nullable = false, length = 30)
    @Enumerated(EnumType.STRING)
    private JobStatus status;

    @Column(name = "message", length = 1000)
    private String message;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    protected JobExecutionHistory() {
    }

    public JobExecutionHistory(JobEntity job, JobStatus status, String message) {
        this.job = job;
        this.status = status;
        this.message = message;
    }

    public Long getId() {
        return id;
    }

    public JobEntity getJob() {
        return job;
    }

    public JobStatus getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
