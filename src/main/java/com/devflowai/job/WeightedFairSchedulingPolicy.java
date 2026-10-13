package com.devflowai.job;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class WeightedFairSchedulingPolicy implements SchedulingPolicy {

    private final int maxJobsPerTenant;

    public WeightedFairSchedulingPolicy(int maxJobsPerTenant) {
        this.maxJobsPerTenant = maxJobsPerTenant;
    }

    @Override
    public List<JobRecord> order(List<JobRecord> jobs) {
        if (jobs == null || jobs.isEmpty()) {
            return List.of();
        }

        List<JobRecord> ordered = new ArrayList<>(jobs);
        ordered.sort(Comparator
                .comparingInt(WeightedFairSchedulingPolicy::effectivePriority).reversed()
                .thenComparing(Comparator.comparingLong(JobRecord::getAgeSeconds).reversed())
                .thenComparing(JobRecord::getQueuedAt)
                .thenComparing(JobRecord::getId));

        return ordered;
    }

    private static int effectivePriority(JobRecord job) {
        int agePromotion = (int) Math.max(0, job.getAgeSeconds() / 300);
        return Math.min(JobPriority.CRITICAL.ordinal(), job.getPriority().ordinal() + agePromotion);
    }

    public int getMaxJobsPerTenant() {
        return maxJobsPerTenant;
    }
}
