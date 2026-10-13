package com.devflowai.service;

import com.devflowai.config.JobSchedulingConfig;
import com.devflowai.domain.JobEntity;
import com.devflowai.domain.JobExecutionHistory;
import com.devflowai.domain.OutboxEvent;
import com.devflowai.domain.TenantQuota;
import com.devflowai.event.JobEvent;
import com.devflowai.job.JobPriority;
import com.devflowai.job.JobStatus;
import com.devflowai.repository.JobExecutionHistoryRepository;
import com.devflowai.repository.JobRepository;
import com.devflowai.repository.OutboxEventRepository;
import com.devflowai.repository.TenantQuotaRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.MDC;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.autoconfigure.validation.ValidationAutoConfiguration;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.data.domain.PageRequest;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DataJpaTest(properties = {"spring.flyway.enabled=false", "devflow.worker.claim-batch-size=3"})
@Import({JobExecutionService.class, RetryPolicy.class, JobSchedulingConfig.class,
        JacksonAutoConfiguration.class, ValidationAutoConfiguration.class,
        JobExecutionServiceTest.TestExecutionConfiguration.class})
class JobExecutionServiceTest {
    @Autowired
    private JobRepository jobRepository;
    @Autowired
    private TenantQuotaRepository quotaRepository;
    @Autowired
    private JobExecutionHistoryRepository historyRepository;
    @Autowired
    private OutboxEventRepository outboxRepository;
    @Autowired
    private JobExecutionService executionService;
    @Autowired
    private RecordingJobHandler handler;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private MeterRegistry metrics;

    @BeforeEach
    void setUp() {
        handler.reset();
    }

    @AfterEach
    void cleanUp() {
        historyRepository.deleteAll();
        outboxRepository.deleteAll();
        jobRepository.deleteAll();
        quotaRepository.deleteAll();
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void duplicateEventsDoNotExecuteOrIncrementAttemptsTwice() throws Exception {
        JobEntity job = createQueuedJob(2);
        JobEvent event = eventFor(job);

        executionService.consume(event);
        executionService.consume(event);

        JobEntity completed = jobRepository.findById(job.getId()).orElseThrow();
        assertEquals(JobStatus.SUCCEEDED, completed.getStatus());
        assertEquals(1, completed.getAttemptCount());
        assertEquals("{}", completed.getResult());
        assertEquals(1, handler.invocations());
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void workerMdcContainsJobContextAndIsClearedAfterExecution() throws Exception {
        JobEntity job = createQueuedJob(2);

        executionService.consume(eventFor(job));

        assertEquals(job.getId().toString(), handler.lastJobId());
        assertEquals(job.getCorrelationId(), handler.lastCorrelationId());
        assertNull(MDC.get("jobId"));
        assertNull(MDC.get("correlationId"));
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void recordsProcessingQueueAgeAndBoundedOutcomeMetrics() throws Exception {
        JobEntity job = createQueuedJob(2);
        long previousProcessingCount = metrics.timer(
                "devflow.worker.job.processing", "outcome", "succeeded").count();
        long previousQueueAgeCount = metrics.timer("devflow.worker.job.queue.age").count();
        double previousClaimedCount = metrics.counter("devflow.worker.claimed").count();

        executionService.consume(eventFor(job));

        assertEquals(previousProcessingCount + 1,
                metrics.timer("devflow.worker.job.processing", "outcome", "succeeded").count());
        assertEquals(previousQueueAgeCount + 1, metrics.timer("devflow.worker.job.queue.age").count());
        assertEquals(previousClaimedCount + 1, metrics.counter("devflow.worker.claimed").count());
        assertEquals(0, metrics.get("devflow.worker.active").gauge().value());
        assertTrue(metrics.getMeters().stream()
                .flatMap(meter -> meter.getId().getTags().stream())
                .noneMatch(tag -> tag.getKey().equals("tenantId")
                        || tag.getKey().equals("jobId")
                        || tag.getKey().equals("correlationId")));
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void executionFailureSchedulesBoundedRetryThenCompletes() throws Exception {
        handler.failFirstAttempt(true);
        JobEntity job = createQueuedJob(2);
        JobEvent firstEvent = eventFor(job);

        executionService.consume(firstEvent);
        JobEntity failedAttempt = jobRepository.findById(job.getId()).orElseThrow();
        assertEquals(JobStatus.RETRY_SCHEDULED, failedAttempt.getStatus());
        assertEquals(1, failedAttempt.getAttemptCount());

        failedAttempt.setScheduledAt(Instant.now().minusSeconds(10));
        jobRepository.saveAndFlush(failedAttempt);
        assertEquals(1, executionService.scheduleDueRetries());

        OutboxEvent retryOutbox = outboxRepository.findAll(PageRequest.of(0, 10)).getContent().getFirst();
        JobEvent retryEvent = objectMapper.readValue(retryOutbox.getPayload(), JobEvent.class);
        executionService.consume(retryEvent);

        JobEntity completed = jobRepository.findById(job.getId()).orElseThrow();
        assertEquals(JobStatus.SUCCEEDED, completed.getStatus());
        assertEquals(2, completed.getAttemptCount());
        assertEquals(2, handler.invocations());
        assertTrue(historyRepository.findByJobIdOrderByCreatedAtAsc(job.getId()).size() >= 5);
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void finalFailureMovesJobToDeadLetter() throws Exception {
        handler.alwaysFail(true);
        JobEntity job = createQueuedJob(1);

        executionService.consume(eventFor(job));

        JobEntity deadLetter = jobRepository.findById(job.getId()).orElseThrow();
        assertEquals(JobStatus.DEAD_LETTER, deadLetter.getStatus());
        assertEquals(1, deadLetter.getAttemptCount());
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void expiredExecutionLeaseIsRecoveredAndRetryGetsNewOutboxEvent() {
        JobEntity job = createQueuedJob(2);
        Instant now = Instant.now();
        assertTrue(job.claim("expired-worker", now.minusSeconds(2), now.minusSeconds(3)));
        jobRepository.saveAndFlush(job);

        assertEquals(1, executionService.recoverExpiredLeases());
        assertEquals(JobStatus.RETRY_SCHEDULED,
                jobRepository.findById(job.getId()).orElseThrow().getStatus());

        job = jobRepository.findById(job.getId()).orElseThrow();
        job.setScheduledAt(Instant.now().minusSeconds(1));
        jobRepository.saveAndFlush(job);
        assertEquals(1, executionService.scheduleDueRetries());
        assertEquals(1, outboxRepository.count());
        assertEquals(JobStatus.QUEUED, jobRepository.findById(job.getId()).orElseThrow().getStatus());
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void concurrentRedeliveryCannotClaimSameJobTwice() throws Exception {
        handler.blockExecution(true);
        JobEntity job = createQueuedJob(2);
        JobEvent event = eventFor(job);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> first = executor.submit(() -> {
                start.await();
                executionService.consume(event);
                return null;
            });
            Future<?> second = executor.submit(() -> {
                start.await();
                executionService.consume(event);
                return null;
            });
            start.countDown();
            assertTrue(handler.awaitStarted());
            handler.releaseExecution();
            first.get();
            second.get();

            JobEntity completed = jobRepository.findById(job.getId()).orElseThrow();
            assertEquals(JobStatus.SUCCEEDED, completed.getStatus());
            assertEquals(1, completed.getAttemptCount());
            assertEquals(1, handler.invocations());
        } finally {
            handler.releaseExecution();
            executor.shutdownNow();
        }
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void oneEventProcessesOnlyOneSchedulerBatch() throws Exception {
        List<JobEntity> jobs = createQueuedJobsAcrossTenants(30);

        executionService.consume(eventFor(jobs.getFirst()));

        assertEquals(3, handler.invocations());
        assertEquals(27, jobRepository.countByStatus(JobStatus.QUEUED));

        executionService.consume(eventFor(jobs.getLast()));

        assertEquals(6, handler.invocations());
        assertEquals(24, jobRepository.countByStatus(JobStatus.QUEUED));
        assertEquals(3, metrics.summary("devflow.worker.claim.batch.size").max());
    }

    private JobEntity createQueuedJob(int maxAttempts) {
        quotaRepository.saveAndFlush(new TenantQuota("tenant-worker", 4, 8));
        JobEntity job = new JobEntity("tenant-worker", "task", "{}", JobPriority.NORMAL, 2,
                maxAttempts, UUID.randomUUID().toString());
        job.transitionTo(JobStatus.QUEUED);
        return jobRepository.saveAndFlush(job);
    }

    private List<JobEntity> createQueuedJobsAcrossTenants(int count) {
        List<JobEntity> jobs = new ArrayList<>();
        int tenantCount = (count + 4) / 5;
        for (int tenantNumber = 0; tenantNumber < tenantCount; tenantNumber++) {
            String tenantId = "tenant-" + tenantNumber;
            quotaRepository.saveAndFlush(new TenantQuota(tenantId, 5, 100));
            for (int jobNumber = 0; jobNumber < 5 && jobs.size() < count; jobNumber++) {
                JobEntity job = new JobEntity(tenantId, "task", "{}", JobPriority.NORMAL, 1,
                        2, UUID.randomUUID().toString());
                job.transitionTo(JobStatus.QUEUED);
                job.setScheduledAt(Instant.now());
                jobs.add(jobRepository.saveAndFlush(job));
            }
        }
        return jobs;
    }

    private JobEvent eventFor(JobEntity job) {
        return new JobEvent(UUID.randomUUID().toString(), JobEvent.TYPE, JobEvent.CURRENT_SCHEMA_VERSION,
                job.getId(), job.getTenantId(), job.getCorrelationId(), Instant.now());
    }

    @TestConfiguration
    static class TestExecutionConfiguration {
        @Bean
        RecordingJobHandler recordingJobHandler() {
            return new RecordingJobHandler();
        }

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    static class RecordingJobHandler implements JobHandler {
        private final AtomicInteger calls = new AtomicInteger();
        private volatile String lastJobId;
        private volatile String lastCorrelationId;
        private volatile boolean failFirst;
        private volatile boolean alwaysFail;
        private volatile boolean block;
        private volatile CountDownLatch started = new CountDownLatch(1);
        private volatile CountDownLatch release = new CountDownLatch(1);

        @Override
        public boolean supports(String jobType) {
            return true;
        }

        @Override
        public String execute(JobExecutionContext context) throws Exception {
            int attempt = calls.incrementAndGet();
            lastJobId = MDC.get("jobId");
            lastCorrelationId = MDC.get("correlationId");
            started.countDown();
            if (block) {
                release.await();
            }
            if (alwaysFail || (failFirst && attempt == 1)) {
                throw new IllegalStateException("simulated handler failure");
            }
            return context.payload();
        }

        void reset() {
            calls.set(0);
            failFirst = false;
            alwaysFail = false;
            block = false;
            started = new CountDownLatch(1);
            release = new CountDownLatch(1);
        }

        int invocations() { return calls.get(); }
        String lastJobId() { return lastJobId; }
        String lastCorrelationId() { return lastCorrelationId; }
        void failFirstAttempt(boolean enabled) { failFirst = enabled; }
        void alwaysFail(boolean enabled) { alwaysFail = enabled; }
        void blockExecution(boolean enabled) { block = enabled; }
        boolean awaitStarted() throws InterruptedException { return started.await(10, java.util.concurrent.TimeUnit.SECONDS); }
        void releaseExecution() { release.countDown(); }
    }
}
