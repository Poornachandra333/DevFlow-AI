package com.devflowai.service;

import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Profile("kafka")
public class OutboxPollingJob {
    private final OutboxDispatchService dispatcher;

    public OutboxPollingJob(OutboxDispatchService dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Scheduled(fixedDelayString = "${devflow.outbox.poll-interval-ms:1000}")
    public void poll() {
        dispatcher.dispatchAvailable();
    }
}
