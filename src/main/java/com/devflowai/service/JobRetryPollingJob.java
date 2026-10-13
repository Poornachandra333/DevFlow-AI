package com.devflowai.service;

import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Profile("kafka")
public class JobRetryPollingJob {
    private final JobExecutionService executionService;

    public JobRetryPollingJob(JobExecutionService executionService) {
        this.executionService = executionService;
    }

    @Scheduled(fixedDelayString = "${devflow.worker.recovery-interval-ms:5000}")
    public void pollDueRetriesAndExpiredLeases() {
        executionService.scheduleDueRetries();
        executionService.recoverExpiredLeases();
    }
}
