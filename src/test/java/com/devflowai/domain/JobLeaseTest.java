package com.devflowai.domain;

import com.devflowai.job.JobPriority;
import com.devflowai.job.JobStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobLeaseTest {

    @Test
    void fencingTokenRejectsStaleWorkerCompletion() {
        Instant now = Instant.now();
        JobEntity job = new JobEntity("tenant-a", "task", "{}", JobPriority.NORMAL, 1, 2, "lease");
        job.transitionTo(JobStatus.QUEUED);
        assertTrue(job.claim("current-token", now.plusSeconds(30), now));

        assertFalse(job.complete("stale-token", "wrong result", now.plusSeconds(1)));
        assertEquals(JobStatus.RUNNING, job.getStatus());
        assertTrue(job.complete("current-token", "valid result", now.plusSeconds(2)));
        assertEquals(JobStatus.SUCCEEDED, job.getStatus());
        assertEquals("valid result", job.getResult());
    }

    @Test
    void expiredLeaseCannotCompleteAndCanOnlyBeRecoveredByRecoveryPath() {
        Instant now = Instant.now();
        JobEntity job = new JobEntity("tenant-a", "task", "{}", JobPriority.NORMAL, 1, 2, "lease");
        job.transitionTo(JobStatus.QUEUED);
        assertTrue(job.claim("worker-token", now.plusSeconds(1), now));

        Instant expiredAt = now.plusSeconds(1);
        assertFalse(job.complete("worker-token", "late result", expiredAt));
        assertTrue(job.recoverExpiredLease("worker-token", "lease expired",
                expiredAt.plusSeconds(1), expiredAt));
        assertEquals(JobStatus.RETRY_SCHEDULED, job.getStatus());
    }

    @Test
    void queuedJobCanBeClaimedEvenWhenStoredScheduleTimestampIsSlightlyAhead() {
        Instant now = Instant.now();
        JobEntity job = new JobEntity("tenant-a", "task", "{}", JobPriority.NORMAL, 1, 2, "ready");
        job.setScheduledAt(now.plusSeconds(1));
        job.transitionTo(JobStatus.QUEUED);

        assertTrue(job.claim("worker-token", now.plusSeconds(30), now));
    }

    @Test
    void retryScheduledJobCannotBeClaimedBeforeItsRetryTime() {
        Instant now = Instant.now();
        JobEntity job = new JobEntity("tenant-a", "task", "{}", JobPriority.NORMAL, 1, 2, "retry");
        job.transitionTo(JobStatus.QUEUED);
        assertTrue(job.claim("first-token", now.plusSeconds(30), now));
        assertTrue(job.fail("first-token", "retry", now.plusSeconds(10), now));

        assertFalse(job.claim("retry-token", now.plusSeconds(30), now.plusSeconds(5)));
        assertEquals(JobStatus.RETRY_SCHEDULED, job.getStatus());
    }
}
