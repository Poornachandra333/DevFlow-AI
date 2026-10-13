package com.devflowai.api.dto;

import com.devflowai.job.JobPriority;
import com.devflowai.job.JobStatus;

import java.time.Instant;

public class JobResponse {
    private Long id;
    private String tenantId;
    private String jobType;
    private JobStatus status;
    private JobPriority priority;
    private int resourceUnits;
    private int maxAttempts;
    private int attemptCount;
    private String payload;
    private String idempotencyKey;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant startedAt;
    private Instant completedAt;
    private String failureReason;
    private String result;
    private String correlationId;

    public JobResponse() {
    }

    public JobResponse(Long id, String tenantId, String jobType, JobStatus status, JobPriority priority,
                       int resourceUnits, int maxAttempts, int attemptCount, String payload,
                       String idempotencyKey, Instant createdAt, Instant updatedAt, Instant startedAt,
                       Instant completedAt, String failureReason) {
        this.id = id;
        this.tenantId = tenantId;
        this.jobType = jobType;
        this.status = status;
        this.priority = priority;
        this.resourceUnits = resourceUnits;
        this.maxAttempts = maxAttempts;
        this.attemptCount = attemptCount;
        this.payload = payload;
        this.idempotencyKey = idempotencyKey;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.startedAt = startedAt;
        this.completedAt = completedAt;
        this.failureReason = failureReason;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getTenantId() { return tenantId; }
    public void setTenantId(String tenantId) { this.tenantId = tenantId; }
    public String getJobType() { return jobType; }
    public void setJobType(String jobType) { this.jobType = jobType; }
    public JobStatus getStatus() { return status; }
    public void setStatus(JobStatus status) { this.status = status; }
    public JobPriority getPriority() { return priority; }
    public void setPriority(JobPriority priority) { this.priority = priority; }
    public int getResourceUnits() { return resourceUnits; }
    public void setResourceUnits(int resourceUnits) { this.resourceUnits = resourceUnits; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public int getAttemptCount() { return attemptCount; }
    public void setAttemptCount(int attemptCount) { this.attemptCount = attemptCount; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
    public Instant getStartedAt() { return startedAt; }
    public void setStartedAt(Instant startedAt) { this.startedAt = startedAt; }
    public Instant getCompletedAt() { return completedAt; }
    public void setCompletedAt(Instant completedAt) { this.completedAt = completedAt; }
    public String getFailureReason() { return failureReason; }
    public void setFailureReason(String failureReason) { this.failureReason = failureReason; }
    public String getResult() { return result; }
    public void setResult(String result) { this.result = result; }
    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
}
