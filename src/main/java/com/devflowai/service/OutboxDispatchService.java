package com.devflowai.service;

import com.devflowai.domain.OutboxEvent;
import com.devflowai.event.JobEvent;
import com.devflowai.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Gauge;
import org.slf4j.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

@Service
public class OutboxDispatchService {
    private static final Logger log = LoggerFactory.getLogger(OutboxDispatchService.class);
    private static final int BATCH_SIZE = 100;
    private static final int MAX_ATTEMPTS = 12;
    private static final Duration CLAIM_LEASE = Duration.ofSeconds(60);

    private final OutboxEventRepository repository;
    private final JobEventPublisher publisher;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactions;
    private final RetryPolicy retryPolicy;
    private final Clock clock;
    private final MeterRegistry meterRegistry;

    public OutboxDispatchService(OutboxEventRepository repository, JobEventPublisher publisher,
                                 ObjectMapper objectMapper, PlatformTransactionManager transactionManager,
                                 RetryPolicy retryPolicy, Clock clock, MeterRegistry meterRegistry) {
        this.repository = repository;
        this.publisher = publisher;
        this.objectMapper = objectMapper;
        this.transactions = new TransactionTemplate(transactionManager);
        this.retryPolicy = retryPolicy;
        this.clock = clock;
        this.meterRegistry = meterRegistry;
        Gauge.builder("devflow.outbox.pending", repository,
                store -> store.countByStatus(OutboxEvent.Status.PENDING)).register(meterRegistry);
        Gauge.builder("devflow.outbox.dead-lettered", repository,
                store -> store.countByStatus(OutboxEvent.Status.DEAD_LETTER)).register(meterRegistry);
    }

    public int dispatchAvailable() {
        List<ClaimedEvent> batch = claimBatch();
        int published = 0;
        for (ClaimedEvent claimed : batch) {
            MDC.put("eventId", claimed.eventId());
            try {
                JobEvent event = objectMapper.readValue(claimed.payload(), JobEvent.class);
                publisher.publish(event);
                boolean delivered = Boolean.TRUE.equals(transactions.execute(status ->
                        repository.findById(claimed.eventId())
                                .map(row -> row.markDelivered(claimed.owner(), clock.instant()))
                                .orElse(false)));
                if (delivered) {
                    meterRegistry.counter("devflow.outbox.published").increment();
                    published++;
                } else {
                    log.warn("Outbox publish acknowledgement arrived after lease ownership changed; eventId={}",
                            claimed.eventId());
                }
            } catch (Exception ex) {
                recordFailure(claimed, ex);
            } finally {
                MDC.remove("eventId");
            }
        }
        return published;
    }

    private List<ClaimedEvent> claimBatch() {
        return transactions.execute(status -> {
            var now = clock.instant();
            var rows = repository.findAvailableForUpdate(now, PageRequest.of(0, BATCH_SIZE));
            if (rows.isEmpty()) {
                return List.of();
            }
            String owner = UUID.randomUUID().toString();
            for (OutboxEvent row : rows) {
                row.claim(owner, now.plus(CLAIM_LEASE));
            }
            repository.saveAll(rows);
            return rows.stream().map(row -> new ClaimedEvent(row.getEventId(), row.getPayload(), owner)).toList();
        });
    }

    private void recordFailure(ClaimedEvent claimed, Exception failure) {
        String message = failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
        Boolean recorded = transactions.execute(status ->
                repository.findById(claimed.eventId()).map(row -> row.recordFailure(
                        claimed.owner(), message,
                        clock.instant().plus(retryPolicy.delayForAttempt(row.getAttemptCount())),
                        MAX_ATTEMPTS)).orElse(false));
        if (Boolean.TRUE.equals(recorded)) {
            meterRegistry.counter("devflow.outbox.failed").increment();
            log.warn("Outbox publish failed; eventId={}, willRetry={}, error={}",
                    claimed.eventId(), repository.findById(claimed.eventId())
                            .map(row -> row.getStatus() == OutboxEvent.Status.PENDING).orElse(false), message);
        }
    }

    private record ClaimedEvent(String eventId, String payload, String owner) {
    }
}
