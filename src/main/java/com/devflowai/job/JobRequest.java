package com.devflowai.job;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;

import java.time.Instant;

public class JobRequest {
    @NotBlank(message = "tenantId is required")
    private String tenantId;

    @NotBlank(message = "jobName is required")
    private String jobName;

    @NotBlank(message = "payload is required")
    private String payload;

    @NotNull(message = "priority is required")
    private JobPriority priority = JobPriority.NORMAL;

    @Positive(message = "resourceUnits must be positive")
    private int resourceUnits = 1;

    @PositiveOrZero(message = "maxAttempts cannot be negative")
    private int maxAttempts = 3;

    @NotBlank(message = "idempotencyKey is required")
    private String idempotencyKey;

    private Instant requestedAt = Instant.now();

    public JobRequest() {
    }

    public JobRequest(String tenantId, String jobName, String payload, JobPriority priority,
                      int resourceUnits, int maxAttempts, String idempotencyKey) {
        this.tenantId = tenantId;
        this.jobName = jobName;
        this.payload = payload;
        this.priority = priority == null ? JobPriority.NORMAL : priority;
        this.resourceUnits = resourceUnits;
        this.maxAttempts = maxAttempts;
        this.idempotencyKey = idempotencyKey;
        this.requestedAt = Instant.now();
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public JobPriority getPriority() {
        return priority;
    }

    public void setPriority(JobPriority priority) {
        this.priority = priority == null ? JobPriority.NORMAL : priority;
    }

    public int getResourceUnits() {
        return resourceUnits;
    }

    public void setResourceUnits(int resourceUnits) {
        this.resourceUnits = resourceUnits;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public Instant getRequestedAt() {
        return requestedAt;
    }

    public void setRequestedAt(Instant requestedAt) {
        this.requestedAt = requestedAt;
    }
}
