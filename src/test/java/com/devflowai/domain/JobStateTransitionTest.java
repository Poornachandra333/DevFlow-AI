package com.devflowai.domain;

import com.devflowai.job.JobPriority;
import com.devflowai.job.JobStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JobStateTransitionTest {

    @Test
    void rejectsTransitionsThatSkipRequiredStates() {
        JobEntity job = new JobEntity("tenant-a", "task", "{}", JobPriority.NORMAL, 1, 2, "state-test");

        assertThrows(IllegalStateException.class, () -> job.transitionTo(JobStatus.RUNNING));
        assertEquals(JobStatus.ACCEPTED, job.getStatus());
    }

    @Test
    void terminalSuccessCannotTransitionAgain() {
        JobEntity job = new JobEntity("tenant-a", "task", "{}", JobPriority.NORMAL, 1, 2, "terminal-test");
        job.transitionTo(JobStatus.QUEUED);
        job.transitionTo(JobStatus.RUNNING);
        job.transitionTo(JobStatus.SUCCEEDED);

        assertThrows(IllegalStateException.class, () -> job.transitionTo(JobStatus.QUEUED));
        assertEquals(JobStatus.SUCCEEDED, job.getStatus());
    }

    @Test
    void failedJobCanOnlyReenterThroughScheduledRetry() {
        JobEntity job = new JobEntity("tenant-a", "task", "{}", JobPriority.NORMAL, 1, 2, "retry-transition");
        job.transitionTo(JobStatus.QUEUED);
        job.transitionTo(JobStatus.RUNNING);
        job.transitionTo(JobStatus.FAILED);

        assertThrows(IllegalStateException.class, () -> job.transitionTo(JobStatus.RUNNING));
        job.transitionTo(JobStatus.RETRY_SCHEDULED);
        assertEquals(JobStatus.RETRY_SCHEDULED, job.getStatus());
    }
}
