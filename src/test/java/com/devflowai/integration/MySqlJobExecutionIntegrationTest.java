package com.devflowai.integration;

import com.devflowai.api.dto.JobSubmissionRequest;
import com.devflowai.config.WorkerProperties;
import com.devflowai.domain.JobEntity;
import com.devflowai.domain.OutboxEvent;
import com.devflowai.domain.TenantQuota;
import com.devflowai.event.JobEvent;
import com.devflowai.job.JobPriority;
import com.devflowai.job.JobRecord;
import com.devflowai.job.JobScheduler;
import com.devflowai.job.JobStatus;
import com.devflowai.repository.OutboxEventRepository;
import com.devflowai.repository.JobRepository;
import com.devflowai.service.JobExecutionContext;
import com.devflowai.service.JobExecutionService;
import com.devflowai.service.JobHandler;
import com.devflowai.service.JobService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.data.domain.PageRequest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@SpringBootTest(properties = {
        "devflow.worker.claim-batch-size=10",
        "devflow.scheduling.max-concurrent-jobs=10"
})
@ActiveProfiles("production")
class MySqlJobExecutionIntegrationTest {
    @Container
    private static final MySQLContainer MYSQL =
            new MySQLContainer(DockerImageName.parse("mysql:8.4"))
                    .withDatabaseName("devflow_it")
                    .withUsername("devflow")
                    .withPassword("devflow-test-only");

    @Autowired
    private JobService jobService;
    @Autowired
    private JobRepository jobRepository;
    @Autowired
    private OutboxEventRepository outboxRepository;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private JobExecutionService executionService;
    @Autowired
    private JobScheduler scheduler;
    @Autowired
    private WorkerProperties workerProperties;
    @Autowired
    private PlatformTransactionManager transactionManager;
    @Autowired
    private IntegrationJobHandler handler;

    @DynamicPropertySource
    static void configureMySql(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.datasource.driver-class-name", MYSQL::getDriverClassName);
    }

    @Test
    void submissionPersistsJobAndOutboxAndIdempotentRetryReturnsSameJob() {
        String tenant = "mysql-transaction-" + UUID.randomUUID();
        String idempotencyKey = UUID.randomUUID().toString();
        JobSubmissionRequest request = request(idempotencyKey);

        var first = jobService.submitJob(tenant, request);
        var duplicate = jobService.submitJob(tenant, request);

        assertEquals(first.getId(), duplicate.getId());
        assertEquals(1, jobRepository.countByTenantId(tenant));
        assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE job_id = ?", Integer.class, first.getId()));
    }

    @Test
    void failedTransactionRollsBackJobAndItsOutboxRows() {
        String tenant = "mysql-rollback-" + UUID.randomUUID();
        String idempotencyKey = UUID.randomUUID().toString();
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);
        AtomicReference<Long> generatedJobId = new AtomicReference<>();

        assertThrows(IllegalStateException.class, () -> transaction.execute(status -> {
            JobEntity job = new JobEntity(tenant, "integration-task", "{}", JobPriority.NORMAL,
                    1, 2, idempotencyKey);
            job.transitionTo(JobStatus.QUEUED);
            generatedJobId.set(jobRepository.saveAndFlush(job).getId());
            String eventId = UUID.randomUUID().toString();
            outboxRepository.saveAndFlush(new OutboxEvent(eventId, JobEvent.TYPE,
                    JobEvent.CURRENT_SCHEMA_VERSION, generatedJobId.get(), job.getCorrelationId(),
                    "{}", Instant.now()));
            throw new IllegalStateException("test rollback");
        }));

        assertEquals(0, jobRepository.countByTenantId(tenant));
        assertEquals(0, jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE job_id = ?", Integer.class, generatedJobId.get()));
    }

    @Test
    void concurrentDuplicateDeliveryCannotExecutePersistedJobTwice() throws Exception {
        String tenant = "mysql-concurrent-" + UUID.randomUUID();
        jobRepository.flush();
        var first = jobService.submitJob(tenant, request(UUID.randomUUID().toString()));
        JobEvent firstEvent = eventFor(first.getId(), tenant, first.getCorrelationId());

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> one = executor.submit(() -> {
                start.await();
                executionService.consume(firstEvent);
                return null;
            });
            Future<?> two = executor.submit(() -> {
                start.await();
                executionService.consume(firstEvent);
                return null;
            });
            start.countDown();
            one.get(30, TimeUnit.SECONDS);
            two.get(30, TimeUnit.SECONDS);

            assertEquals(JobStatus.SUCCEEDED, jobRepository.findById(first.getId()).orElseThrow().getStatus());
            assertEquals(1, handler.invocations(first.getId()));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void concurrentConsumersClaimDistinctJobsWithinCapacity() throws Exception {
        String tenant = "mysql-distinct-" + UUID.randomUUID();
        var first = jobService.submitJob(tenant, request(UUID.randomUUID().toString()));
        var second = jobService.submitJob(tenant, request(UUID.randomUUID().toString()));
        JobEvent firstEvent = eventFor(first.getId(), tenant, first.getCorrelationId());
        JobEvent secondEvent = eventFor(second.getId(), tenant, second.getCorrelationId());
        List<JobEntity> candidates = jobRepository.findDispatchCandidates(
                List.of(JobStatus.QUEUED, JobStatus.RETRY_SCHEDULED), Instant.now(), PageRequest.of(0, 10));
        assertEquals(10, scheduler.getMaxConcurrentJobs());
        assertEquals(5, scheduler.getMaxConcurrentPerTenant());
        assertEquals(10, workerProperties.getClaimBatchSize());
        assertTrue(candidates.stream().anyMatch(job -> job.getId().equals(first.getId())));
        assertTrue(candidates.stream().anyMatch(job -> job.getId().equals(second.getId())));
        Instant now = Instant.now();
        List<JobRecord> records = candidates.stream()
                .filter(job -> job.getId().equals(first.getId()) || job.getId().equals(second.getId()))
                .map(job -> new JobRecord(job.getId().toString(), tenant, job.getJobType(), job.getPriority(),
                        job.getResourceUnits(), job.getCreatedAt(), job.getScheduledAt(),
                        Math.max(0, Duration.between(job.getCreatedAt(), now).getSeconds()),
                        job.getAttemptCount(), job.getStatus()))
                .toList();
        assertEquals(2, scheduler.schedule(records, 0, Map.of(), Map.of(), Map.of(tenant, 50)).size());

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> one = executor.submit(() -> {
                start.await();
                executionService.consume(firstEvent);
                return null;
            });
            Future<?> two = executor.submit(() -> {
                start.await();
                executionService.consume(secondEvent);
                return null;
            });
            start.countDown();
            one.get(30, TimeUnit.SECONDS);
            two.get(30, TimeUnit.SECONDS);

            assertEquals(JobStatus.SUCCEEDED, jobRepository.findById(first.getId()).orElseThrow().getStatus());
            assertEquals(JobStatus.SUCCEEDED, jobRepository.findById(second.getId()).orElseThrow().getStatus());
            assertEquals(1, handler.invocations(first.getId()));
            assertEquals(1, handler.invocations(second.getId()));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void expiredWorkerLeaseIsRecoveredAndRetryCanBeClaimedAfterStaleWorkerReturns() {
        String tenant = "mysql-lease-recovery-" + UUID.randomUUID();
        var submission = jobService.submitJob(tenant, request(UUID.randomUUID().toString()));
        String staleToken = UUID.randomUUID().toString();
        Instant now = Instant.now();
        JobEntity running = jobRepository.findById(submission.getId()).orElseThrow();
        assertTrue(running.claim(staleToken, now.minusSeconds(1), now.minusSeconds(2)));
        jobRepository.saveAndFlush(running);

        assertEquals(1, executionService.recoverExpiredLeases());
        JobEntity recovered = jobRepository.findById(submission.getId()).orElseThrow();
        assertEquals(JobStatus.RETRY_SCHEDULED, recovered.getStatus());
        assertFalse(recovered.complete(staleToken, "stale result", Instant.now()));
        assertEquals(JobStatus.RETRY_SCHEDULED,
                jobRepository.findById(submission.getId()).orElseThrow().getStatus());

        jdbcTemplate.update("""
                UPDATE jobs
                SET scheduled_at = DATE_SUB(UTC_TIMESTAMP(), INTERVAL 5 SECOND)
                WHERE id = ?
                """, submission.getId());
        assertEquals(1, jdbcTemplate.queryForObject("""
                SELECT COUNT(*) FROM jobs
                WHERE id = ? AND status = 'RETRY_SCHEDULED' AND scheduled_at <= UTC_TIMESTAMP()
                """, Integer.class, submission.getId()));
        assertEquals(1, executionService.scheduleDueRetries());
        executionService.consume(eventFor(submission.getId(), tenant, recovered.getCorrelationId()));

        JobEntity completed = jobRepository.findById(submission.getId()).orElseThrow();
        assertEquals(JobStatus.SUCCEEDED, completed.getStatus());
        assertEquals(2, completed.getAttemptCount());
        assertEquals(1, handler.invocations(submission.getId()));
        assertEquals(2, jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE job_id = ?", Integer.class, submission.getId()));
    }

    private JobSubmissionRequest request(String idempotencyKey) {
        return new JobSubmissionRequest("integration-task", "{}", JobPriority.NORMAL, 1, 2, idempotencyKey);
    }

    private JobEvent eventFor(Long jobId, String tenant, String correlationId) {
        return new JobEvent(UUID.randomUUID().toString(), JobEvent.TYPE, JobEvent.CURRENT_SCHEMA_VERSION,
                jobId, tenant, correlationId, Instant.now());
    }

    @TestConfiguration
    static class IntegrationConfiguration {
        @Bean
        @Order(Ordered.HIGHEST_PRECEDENCE)
        IntegrationJobHandler integrationJobHandler() {
            return new IntegrationJobHandler();
        }
    }

    static class IntegrationJobHandler implements JobHandler {
        private final ConcurrentHashMap<Long, AtomicInteger> invocations = new ConcurrentHashMap<>();

        @Override
        public boolean supports(String jobType) {
            return "integration-task".equals(jobType);
        }

        @Override
        public String execute(JobExecutionContext context) {
            invocations.computeIfAbsent(context.jobId(), ignored -> new AtomicInteger()).incrementAndGet();
            return context.payload();
        }

        int invocations(Long jobId) {
            return invocations.getOrDefault(jobId, new AtomicInteger()).get();
        }
    }
}
