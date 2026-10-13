package com.devflowai.service;

import com.devflowai.config.JobSchedulingConfig;
import com.devflowai.domain.JobEntity;
import com.devflowai.domain.OutboxEvent;
import com.devflowai.event.JobEvent;
import com.devflowai.job.JobPriority;
import com.devflowai.job.JobStatus;
import com.devflowai.repository.JobRepository;
import com.devflowai.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.MDC;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@DataJpaTest(properties = "spring.flyway.enabled=false")
@Import({OutboxDispatchService.class, RetryPolicy.class, JobSchedulingConfig.class,
        JacksonAutoConfiguration.class, OutboxDispatchServiceTest.TestMetricsConfiguration.class})
class OutboxDispatchServiceTest {
    @Autowired
    private JobRepository jobRepository;
    @Autowired
    private OutboxEventRepository outboxRepository;
    @Autowired
    private OutboxDispatchService dispatcher;
    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private JobEventPublisher publisher;

    @AfterEach
    void cleanUp() {
        outboxRepository.deleteAll();
        jobRepository.deleteAll();
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void publishesAndMarksOutboxEventOnlyAfterPublisherAcknowledges() throws Exception {
        OutboxEvent row = createOutboxEvent();

        assertEquals(1, dispatcher.dispatchAvailable());

        OutboxEvent delivered = outboxRepository.findById(row.getEventId()).orElseThrow();
        assertEquals(OutboxEvent.Status.DELIVERED, delivered.getStatus());
        assertEquals(1, delivered.getAttemptCount());
        verify(publisher).publish(any(JobEvent.class));
        assertEquals(0, dispatcher.dispatchAvailable());
        verify(publisher, times(1)).publish(any(JobEvent.class));
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void publisherFailureLeavesEventPendingWithRetryMetadata() throws Exception {
        OutboxEvent row = createOutboxEvent();
        doAnswer(invocation -> {
            assertEquals(row.getEventId(), MDC.get("eventId"));
            throw new IllegalStateException("broker unavailable");
        }).when(publisher).publish(any(JobEvent.class));

        assertEquals(0, dispatcher.dispatchAvailable());
        assertNull(MDC.get("eventId"));

        OutboxEvent retry = outboxRepository.findById(row.getEventId()).orElseThrow();
        assertEquals(OutboxEvent.Status.PENDING, retry.getStatus());
        assertEquals(1, retry.getAttemptCount());
        assertEquals("broker unavailable", retry.getLastError());
        assertEquals(1, outboxRepository.countByStatus(OutboxEvent.Status.PENDING));
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void expiredPublisherLeaseCanBeReclaimedWithSameStableEventId() throws Exception {
        OutboxEvent row = createOutboxEvent();
        row.claim("dead-publisher", Instant.now().minusSeconds(1));
        outboxRepository.saveAndFlush(row);

        assertEquals(1, dispatcher.dispatchAvailable());
        verify(publisher).publish(any(JobEvent.class));
        OutboxEvent delivered = outboxRepository.findById(row.getEventId()).orElseThrow();
        assertEquals(OutboxEvent.Status.DELIVERED, delivered.getStatus());
        assertEquals(2, delivered.getAttemptCount());
    }

    private OutboxEvent createOutboxEvent() throws Exception {
        JobEntity job = new JobEntity("tenant-outbox", "task", "{}", JobPriority.NORMAL, 1, 2,
                UUID.randomUUID().toString());
        job.transitionTo(JobStatus.QUEUED);
        jobRepository.saveAndFlush(job);
        String eventId = UUID.randomUUID().toString();
        JobEvent event = new JobEvent(eventId, JobEvent.TYPE, JobEvent.CURRENT_SCHEMA_VERSION,
                job.getId(), job.getTenantId(), job.getCorrelationId(), Instant.now());
        return outboxRepository.saveAndFlush(new OutboxEvent(eventId, event.eventType(),
                event.schemaVersion(), event.jobId(), event.correlationId(),
                objectMapper.writeValueAsString(event), Instant.now()));
    }

    @TestConfiguration
    static class TestMetricsConfiguration {
        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }
}
