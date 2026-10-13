package com.devflowai.config;

import com.devflowai.job.JobScheduler;
import com.devflowai.job.WeightedFairSchedulingPolicy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration
@EnableConfigurationProperties(WorkerProperties.class)
public class JobSchedulingConfig {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    public WeightedFairSchedulingPolicy weightedFairSchedulingPolicy(
            @Value("${devflow.scheduling.max-concurrent-per-tenant:5}") int maxJobsPerTenant) {
        return new WeightedFairSchedulingPolicy(maxJobsPerTenant);
    }

    @Bean
    public JobScheduler jobScheduler(
            Clock clock,
            WeightedFairSchedulingPolicy weightedFairSchedulingPolicy,
            @Value("${devflow.scheduling.max-concurrent-jobs:25}") int maxConcurrentJobs,
            @Value("${devflow.scheduling.max-concurrent-per-tenant:5}") int maxConcurrentPerTenant) {
        return new JobScheduler(clock, weightedFairSchedulingPolicy, maxConcurrentJobs, maxConcurrentPerTenant);
    }
}
