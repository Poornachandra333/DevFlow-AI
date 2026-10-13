package com.devflowai.api.dto;

import com.devflowai.job.JobPriority;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;

public class JobSubmissionRequest {

    @NotBlank(message = "jobType is required")
    private String jobType;

    @NotBlank(message = "payload is required")
    private String payload;

    @NotNull(message = "priority is required")
    private JobPriority priority = JobPriority.NORMAL;

    @Positive(message = "resourceUnits must be positive")
    private int resourceUnits = 1;

    @Min(value = 1, message = "maxAttempts must be at least 1")
    @Max(value = 10, message = "maxAttempts must not exceed 10")
    private int maxAttempts = 3;

    @NotBlank(message = "idempotencyKey is required")
    private String idempotencyKey;

    public JobSubmissionRequest() {
    }

    public JobSubmissionRequest(String jobType, String payload, JobPriority priority, int resourceUnits,
                               int maxAttempts, String idempotencyKey) {
        this.jobType = jobType;
        this.payload = payload;
        this.priority = priority == null ? JobPriority.NORMAL : priority;
        this.resourceUnits = resourceUnits;
        this.maxAttempts = maxAttempts;
        this.idempotencyKey = idempotencyKey;
    }

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
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
}
