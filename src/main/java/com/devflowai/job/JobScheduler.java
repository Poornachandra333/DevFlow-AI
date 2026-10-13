package com.devflowai.job;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobScheduler {

    private final Clock clock;
    private final SchedulingPolicy schedulingPolicy;
    private final int maxConcurrentJobs;
    private final int maxConcurrentPerTenant;

    public JobScheduler(Clock clock, SchedulingPolicy schedulingPolicy, int maxConcurrentJobs, int maxConcurrentPerTenant) {
        this.clock = clock;
        this.schedulingPolicy = schedulingPolicy;
        this.maxConcurrentJobs = maxConcurrentJobs;
        this.maxConcurrentPerTenant = maxConcurrentPerTenant;
    }

    public List<JobRecord> schedule(List<JobRecord> jobs) {
        return schedule(jobs, 0, Map.of(), Map.of(), Map.of());
    }

    public List<JobRecord> schedule(List<JobRecord> jobs, int currentlyRunning,
                                    Map<String, Integer> runningByTenant,
                                    Map<String, Integer> resourcesByTenant,
                                    Map<String, Integer> resourceCapsByTenant) {
        if (jobs == null || jobs.isEmpty()) {
            return List.of();
        }

        List<JobRecord> ordered = schedulingPolicy.order(jobs);
        List<JobRecord> selected = new ArrayList<>();
        Map<String, Integer> tenantUsage = new HashMap<>(runningByTenant);
        Map<String, Integer> resourceUsage = new HashMap<>(resourcesByTenant);
        int runningCount = currentlyRunning;

        for (JobRecord job : ordered) {
            if (runningCount >= maxConcurrentJobs) {
                break;
            }

            int tenantCount = tenantUsage.getOrDefault(job.getTenantId(), 0);
            if (tenantCount >= maxConcurrentPerTenant) {
                continue;
            }

            int resourceLimit = resourceCapsByTenant.getOrDefault(job.getTenantId(), Integer.MAX_VALUE);
            int currentResourceUsage = resourceUsage.getOrDefault(job.getTenantId(), 0);
            if (currentResourceUsage + job.getResourceUnits() > resourceLimit) {
                continue;
            }

            if (job.getStatus() == JobStatus.QUEUED || job.getStatus() == JobStatus.RETRY_SCHEDULED) {
                selected.add(job);
                runningCount++;
                tenantUsage.merge(job.getTenantId(), 1, Integer::sum);
                resourceUsage.merge(job.getTenantId(), job.getResourceUnits(), Integer::sum);
            }
        }

        return selected;
    }

    public boolean isStarvationPrevented(JobRecord job) {
        return job.getPriority() == JobPriority.CRITICAL || job.getAgeSeconds() >= 300;
    }

    public JobRecord createJob(String id, String tenantId, String jobName, JobPriority priority,
                               int resourceUnits, Instant createdAt, JobStatus status, int retryCount) {
        long ageSeconds = Duration.between(createdAt, clock.instant()).getSeconds();
        Instant queuedAt = createdAt;
        return new JobRecord(id, tenantId, jobName, priority, resourceUnits, createdAt, queuedAt, ageSeconds, retryCount, status);
    }

    public int getMaxConcurrentJobs() {
        return maxConcurrentJobs;
    }

    public int getMaxConcurrentPerTenant() {
        return maxConcurrentPerTenant;
    }
}
