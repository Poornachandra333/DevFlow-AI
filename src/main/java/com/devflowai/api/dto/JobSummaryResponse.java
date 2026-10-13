package com.devflowai.api.dto;

import com.devflowai.job.JobPriority;
import com.devflowai.job.JobStatus;

import java.time.Instant;

public class JobSummaryResponse {
    private Long id;
    private String tenantId;
    private String jobType;
    private JobStatus status;
    private JobPriority priority;
    private int resourceUnits;
    private Instant createdAt;
    private Instant updatedAt;

    public JobSummaryResponse() {
    }

    public JobSummaryResponse(Long id, String tenantId, String jobType, JobStatus status,
                             JobPriority priority, int resourceUnits, Instant createdAt, Instant updatedAt) {
        this.id = id;
        this.tenantId = tenantId;
        this.jobType = jobType;
        this.status = status;
        this.priority = priority;
        this.resourceUnits = resourceUnits;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
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
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
}
