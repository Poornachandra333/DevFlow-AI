package com.devflowai.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "outbox_events", indexes = {
        @Index(name = "idx_outbox_status_next_attempt", columnList = "status,next_attempt_at"),
        @Index(name = "idx_outbox_job_id", columnList = "job_id")
})
public class OutboxEvent {
    public enum Status {
        PENDING, IN_FLIGHT, DELIVERED, DEAD_LETTER
    }

    @Id
    @Column(name = "event_id", nullable = false, length = 36)
    private String eventId;

    @Column(name = "event_type", nullable = false, length = 100)
    private String eventType;

    @Column(name = "schema_version", nullable = false)
    private int schemaVersion;

    @Column(name = "job_id", nullable = false)
    private Long jobId;

    @Column(name = "correlation_id", nullable = false, length = 36)
    private String correlationId;

    @Column(name = "payload", nullable = false, columnDefinition = "longtext")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private Status status = Status.PENDING;

    @Column(name = "attempt_count", nullable = false)
    private int attemptCount;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "next_attempt_at", nullable = false)
    private Instant nextAttemptAt = Instant.now();

    @Column(name = "lease_owner", length = 36)
    private String leaseOwner;

    @Column(name = "lease_expires_at")
    private Instant leaseExpiresAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(name = "last_error", length = 2000)
    private String lastError;

    protected OutboxEvent() {
    }

    public OutboxEvent(String eventId, String eventType, int schemaVersion, Long jobId,
                       String correlationId, String payload, Instant now) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.schemaVersion = schemaVersion;
        this.jobId = jobId;
        this.correlationId = correlationId;
        this.payload = payload;
        this.createdAt = now;
        this.nextAttemptAt = now;
    }

    public String getEventId() { return eventId; }
    public String getEventType() { return eventType; }
    public int getSchemaVersion() { return schemaVersion; }
    public Long getJobId() { return jobId; }
    public String getCorrelationId() { return correlationId; }
    public String getPayload() { return payload; }
    public Status getStatus() { return status; }
    public int getAttemptCount() { return attemptCount; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getNextAttemptAt() { return nextAttemptAt; }
    public Instant getLeaseExpiresAt() { return leaseExpiresAt; }
    public Instant getPublishedAt() { return publishedAt; }
    public String getLastError() { return lastError; }

    public boolean isAvailableAt(Instant now) {
        return (status == Status.PENDING && !nextAttemptAt.isAfter(now))
                || (status == Status.IN_FLIGHT && leaseExpiresAt != null && !leaseExpiresAt.isAfter(now));
    }

    public void claim(String owner, Instant expiresAt) {
        if (status == Status.DELIVERED || status == Status.DEAD_LETTER) {
            throw new IllegalStateException("Cannot claim completed outbox event " + eventId);
        }
        status = Status.IN_FLIGHT;
        leaseOwner = owner;
        leaseExpiresAt = expiresAt;
        attemptCount++;
    }

    public boolean markDelivered(String owner, Instant now) {
        if (status != Status.IN_FLIGHT || !owner.equals(leaseOwner)) {
            return false;
        }
        status = Status.DELIVERED;
        publishedAt = now;
        leaseOwner = null;
        leaseExpiresAt = null;
        lastError = null;
        return true;
    }

    public boolean recordFailure(String owner, String error, Instant retryAt, int maxAttempts) {
        if (status != Status.IN_FLIGHT || !owner.equals(leaseOwner)) {
            return false;
        }
        lastError = error.length() > 2000 ? error.substring(0, 2000) : error;
        leaseOwner = null;
        leaseExpiresAt = null;
        if (attemptCount >= maxAttempts) {
            status = Status.DEAD_LETTER;
        } else {
            status = Status.PENDING;
            nextAttemptAt = retryAt;
        }
        return true;
    }
}
