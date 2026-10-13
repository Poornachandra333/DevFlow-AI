package com.devflowai.event;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.time.Instant;

public record JobEvent(
        @NotBlank String eventId,
        @NotBlank String eventType,
        @Positive int schemaVersion,
        @Positive Long jobId,
        @NotBlank String tenantId,
        @NotBlank String correlationId,
        @NotNull Instant occurredAt) {

    public static final String TYPE = "job.dispatch.requested";
    public static final int CURRENT_SCHEMA_VERSION = 1;
}
