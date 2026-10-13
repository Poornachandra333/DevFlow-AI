package com.devflowai.service;

import java.time.Instant;

public record JobExecutionContext(
        Long jobId,
        String tenantId,
        String jobType,
        String payload,
        String correlationId,
        String fencingToken,
        int attempt,
        Instant leaseExpiresAt) {
}
