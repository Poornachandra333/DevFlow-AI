package com.devflowai.job;

import java.util.List;

public interface SchedulingPolicy {
    List<JobRecord> order(List<JobRecord> jobs);
}
