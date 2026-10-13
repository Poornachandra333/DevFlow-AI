package com.devflowai.job;

import java.time.Instant;
import java.util.Objects;

public class JobRecord {
    private final String id;
    private final String tenantId;
    private final String jobName;
    private final JobPriority priority;
    private final int resourceUnits;
    private final Instant createdAt;
    private final Instant queuedAt;
    private final long ageSeconds;
    private final int retryCount;
    private final JobStatus status;

    public JobRecord(String id, String tenantId, String jobName, JobPriority priority, int resourceUnits,
                     Instant createdAt, Instant queuedAt, long ageSeconds, int retryCount, JobStatus status) {
        this.id = id;
        this.tenantId = tenantId;
        this.jobName = jobName;
        this.priority = priority;
        this.resourceUnits = resourceUnits;
        this.createdAt = createdAt;
        this.queuedAt = queuedAt;
        this.ageSeconds = ageSeconds;
        this.retryCount = retryCount;
        this.status = status;
    }

    public String getId() {
        return id;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getJobName() {
        return jobName;
    }

    public JobPriority getPriority() {
        return priority;
    }

    public int getResourceUnits() {
        return resourceUnits;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getQueuedAt() {
        return queuedAt;
    }

    public long getAgeSeconds() {
        return ageSeconds;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public JobStatus getStatus() {
        return status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobRecord jobRecord = (JobRecord) o;
        return resourceUnits == jobRecord.resourceUnits && ageSeconds == jobRecord.ageSeconds && retryCount == jobRecord.retryCount && Objects.equals(id, jobRecord.id) && Objects.equals(tenantId, jobRecord.tenantId) && Objects.equals(jobName, jobRecord.jobName) && priority == jobRecord.priority && Objects.equals(createdAt, jobRecord.createdAt) && Objects.equals(queuedAt, jobRecord.queuedAt) && status == jobRecord.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, tenantId, jobName, priority, resourceUnits, createdAt, queuedAt, ageSeconds, retryCount, status);
    }
}
