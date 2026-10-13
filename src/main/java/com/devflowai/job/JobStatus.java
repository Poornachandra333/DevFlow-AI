package com.devflowai.job;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public enum JobStatus {
    ACCEPTED,
    QUEUED,
    RUNNING,
    RETRY_SCHEDULED,
    SUCCEEDED,
    FAILED,
    CANCELLED,
    DEAD_LETTER;

    private static final Map<JobStatus, Set<JobStatus>> ALLOWED_TRANSITIONS = Map.of(
            ACCEPTED, EnumSet.of(QUEUED, CANCELLED),
            QUEUED, EnumSet.of(RUNNING, CANCELLED),
            RUNNING, EnumSet.of(SUCCEEDED, FAILED, CANCELLED),
            RETRY_SCHEDULED, EnumSet.of(QUEUED, RUNNING, CANCELLED),
            FAILED, EnumSet.of(RETRY_SCHEDULED, DEAD_LETTER),
            SUCCEEDED, EnumSet.noneOf(JobStatus.class),
            CANCELLED, EnumSet.noneOf(JobStatus.class),
            DEAD_LETTER, EnumSet.noneOf(JobStatus.class));

    public boolean canTransitionTo(JobStatus nextStatus) {
        return nextStatus != null && ALLOWED_TRANSITIONS.get(this).contains(nextStatus);
    }

    public boolean isTerminal() {
        return switch (this) {
            case SUCCEEDED, FAILED, CANCELLED, DEAD_LETTER -> true;
            default -> false;
        };
    }
}
