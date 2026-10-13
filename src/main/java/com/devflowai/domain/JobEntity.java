package com.devflowai.domain;

import com.devflowai.job.JobPriority;
import com.devflowai.job.JobStatus;
import jakarta.persistence.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "jobs", indexes = {
        @Index(name = "idx_jobs_tenant_created", columnList = "tenant_id, created_at"),
        @Index(name = "idx_jobs_status_priority", columnList = "status, priority"),
        @Index(name = "idx_jobs_idempotency", columnList = "tenant_id, idempotency_key", unique = true)
})
public class JobEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "tenant_id", nullable = false, length = 100)
    private String tenantId;

    @Column(name = "job_type", nullable = false, length = 120)
    private String jobType;

    @Column(name = "status", nullable = false, length = 30)
    @Enumerated(EnumType.STRING)
    private JobStatus status = JobStatus.ACCEPTED;

    @Column(name = "priority", nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    private JobPriority priority = JobPriority.NORMAL;

    @Column(name = "resource_units", nullable = false)
    private int resourceUnits;

    @Column(name = "max_attempts", nullable = false)
    private int maxAttempts = 3;

    @Column(name = "attempt_count", nullable = false)
    private int attemptCount = 0;

    @Column(name = "payload", nullable = false, length = 8000)
    private String payload;

    @Column(name = "idempotency_key", length = 255)
    private String idempotencyKey;

    @Column(name = "correlation_id", nullable = false, length = 36)
    private String correlationId;

    @Column(name = "result", columnDefinition = "longtext")
    private String result;

    @Column(name = "lease_token", length = 36)
    private String leaseToken;

    @Column(name = "lease_expires_at")
    private Instant leaseExpiresAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    @Column(name = "scheduled_at")
    private Instant scheduledAt;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    @Column(name = "failure_reason", length = 1000)
    private String failureReason;

    @OneToMany(mappedBy = "job", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<JobExecutionHistory> executionHistory = new ArrayList<>();

    protected JobEntity() {
    }

    public JobEntity(String tenantId, String jobType, String payload, JobPriority priority,
                    int resourceUnits, int maxAttempts, String idempotencyKey) {
        this.tenantId = tenantId;
        this.jobType = jobType;
        this.payload = payload;
        this.priority = priority;
        this.resourceUnits = resourceUnits;
        this.maxAttempts = maxAttempts;
        this.idempotencyKey = idempotencyKey;
        this.correlationId = java.util.UUID.randomUUID().toString();
    }

    public Long getId() {
        return id;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getJobType() {
        return jobType;
    }

    public JobStatus getStatus() {
        return status;
    }

    public void transitionTo(JobStatus nextStatus) {
        if (!status.canTransitionTo(nextStatus)) {
            throw new IllegalStateException("Invalid job state transition: " + status + " -> " + nextStatus);
        }
        this.status = nextStatus;
        this.updatedAt = Instant.now();
    }

    public JobPriority getPriority() {
        return priority;
    }

    public void setPriority(JobPriority priority) {
        this.priority = priority;
        this.updatedAt = Instant.now();
    }

    public int getResourceUnits() {
        return resourceUnits;
    }

    public void setResourceUnits(int resourceUnits) {
        this.resourceUnits = resourceUnits;
        this.updatedAt = Instant.now();
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public void incrementAttemptCount() {
        this.attemptCount += 1;
        this.updatedAt = Instant.now();
    }

    public String getPayload() {
        return payload;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getResult() {
        return result;
    }

    public String getLeaseToken() {
        return leaseToken;
    }

    public Instant getLeaseExpiresAt() {
        return leaseExpiresAt;
    }

    public boolean claim(String token, Instant expiresAt, Instant now) {
        if (status != JobStatus.QUEUED && status != JobStatus.RETRY_SCHEDULED) {
            return false;
        }
        if (status == JobStatus.RETRY_SCHEDULED && scheduledAt != null && scheduledAt.isAfter(now)) {
            return false;
        }
        transitionTo(JobStatus.RUNNING);
        attemptCount++;
        startedAt = now;
        leaseToken = token;
        leaseExpiresAt = expiresAt;
        updatedAt = now;
        return true;
    }

    public boolean complete(String token, String executionResult, Instant now) {
        if (!isCurrentLease(token, now)) {
            return false;
        }
        transitionTo(JobStatus.SUCCEEDED);
        result = executionResult;
        failureReason = null;
        completedAt = now;
        clearLease();
        return true;
    }

    public boolean fail(String token, String reason, Instant retryAt, Instant now) {
        if (!isCurrentLease(token, now)) {
            return false;
        }
        return finishFailure(reason, retryAt, now);
    }

    public boolean recoverExpiredLease(String token, String reason, Instant retryAt, Instant now) {
        if (!hasLease(token) || leaseExpiresAt.isAfter(now)) {
            return false;
        }
        return finishFailure(reason, retryAt, now);
    }

    private boolean finishFailure(String reason, Instant retryAt, Instant now) {
        transitionTo(JobStatus.FAILED);
        failureReason = reason;
        clearLease();
        if (attemptCount < maxAttempts) {
            transitionTo(JobStatus.RETRY_SCHEDULED);
            scheduledAt = retryAt;
        } else {
            transitionTo(JobStatus.DEAD_LETTER);
            completedAt = now;
        }
        updatedAt = now;
        return true;
    }

    public boolean renewLease(String token, Instant expiresAt, Instant now) {
        if (!isCurrentLease(token, now)) {
            return false;
        }
        leaseExpiresAt = expiresAt;
        updatedAt = now;
        return true;
    }

    private boolean isCurrentLease(String token, Instant now) {
        return hasLease(token) && leaseExpiresAt.isAfter(now);
    }

    private boolean hasLease(String token) {
        return status == JobStatus.RUNNING && leaseToken != null && leaseToken.equals(token)
                && leaseExpiresAt != null;
    }

    private void clearLease() {
        leaseToken = null;
        leaseExpiresAt = null;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public Instant getScheduledAt() {
        return scheduledAt;
    }

    public void setScheduledAt(Instant scheduledAt) {
        this.scheduledAt = scheduledAt;
        this.updatedAt = Instant.now();
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
        this.updatedAt = Instant.now();
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(Instant completedAt) {
        this.completedAt = completedAt;
        this.updatedAt = Instant.now();
    }

    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
        this.updatedAt = Instant.now();
    }

    public List<JobExecutionHistory> getExecutionHistory() {
        return executionHistory;
    }

    public void addExecutionHistory(JobExecutionHistory history) {
        executionHistory.add(history);
        history.getJob();
    }
}
