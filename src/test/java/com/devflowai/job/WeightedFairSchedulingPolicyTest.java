package com.devflowai.job;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WeightedFairSchedulingPolicyTest {

    @Test
    void ordersJobsByPriorityAndAge() {
        Instant now = Instant.now();
        JobRecord oldLow = new JobRecord("job-1", "tenant-a", "low", JobPriority.LOW, 1, now.minusSeconds(60), now.minusSeconds(60), 60, 0, JobStatus.QUEUED);
        JobRecord critical = new JobRecord("job-2", "tenant-b", "critical", JobPriority.CRITICAL, 2, now.minusSeconds(30), now.minusSeconds(30), 30, 0, JobStatus.QUEUED);
        JobRecord high = new JobRecord("job-3", "tenant-a", "high", JobPriority.HIGH, 1, now.minusSeconds(10), now.minusSeconds(10), 10, 0, JobStatus.QUEUED);

        WeightedFairSchedulingPolicy policy = new WeightedFairSchedulingPolicy(3);
        List<JobRecord> ordered = policy.order(List.of(oldLow, critical, high));

        assertEquals("job-2", ordered.get(0).getId());
        assertEquals("job-3", ordered.get(1).getId());
        assertEquals("job-1", ordered.get(2).getId());
    }

    @Test
    void promotesLongWaitingLowPriorityJobToPreventStarvation() {
        Instant now = Instant.now();
        JobRecord longWaitingLow = new JobRecord("old-low", "tenant-a", "low", JobPriority.LOW, 1,
                now.minusSeconds(1_000), now.minusSeconds(1_000), 1_000, 0, JobStatus.QUEUED);
        JobRecord newerCritical = new JobRecord("new-critical", "tenant-b", "critical", JobPriority.CRITICAL, 1,
                now.minusSeconds(100), now.minusSeconds(100), 100, 0, JobStatus.QUEUED);

        List<JobRecord> ordered = new WeightedFairSchedulingPolicy(2).order(List.of(newerCritical, longWaitingLow));

        assertEquals("old-low", ordered.getFirst().getId());
    }

    @Test
    void selectsOnlyJobsWithinGlobalTenantAndAggregateResourceLimits() {
        Instant now = Instant.now();
        JobRecord tenantAResourceBlocked = new JobRecord("a-large", "tenant-a", "large",
                JobPriority.CRITICAL, 3, now, now, 0, 0, JobStatus.QUEUED);
        JobRecord tenantASmall = new JobRecord("a-small", "tenant-a", "small",
                JobPriority.HIGH, 1, now, now, 0, 0, JobStatus.QUEUED);
        JobRecord tenantAExtra = new JobRecord("a-extra", "tenant-a", "extra",
                JobPriority.NORMAL, 1, now, now, 0, 0, JobStatus.QUEUED);
        JobRecord tenantBJob = new JobRecord("b-job", "tenant-b", "work",
                JobPriority.NORMAL, 2, now, now, 0, 0, JobStatus.QUEUED);

        JobScheduler scheduler = new JobScheduler(Clock.systemUTC(),
                new WeightedFairSchedulingPolicy(2), 3, 2);
        List<JobRecord> selected = scheduler.schedule(
                List.of(tenantAResourceBlocked, tenantASmall, tenantAExtra, tenantBJob),
                1,
                Map.of("tenant-a", 1),
                Map.of("tenant-a", 8),
                Map.of("tenant-a", 10, "tenant-b", 10));

        assertEquals(List.of("a-small", "b-job"), selected.stream().map(JobRecord::getId).toList());
    }
}
