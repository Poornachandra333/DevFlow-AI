package com.devflowai.integration;

import com.devflowai.DevFlowAiApplication;
import com.devflowai.api.dto.JobSubmissionRequest;
import com.devflowai.api.dto.JobResponse;
import com.devflowai.domain.JobEntity;
import com.devflowai.domain.OutboxEvent;
import com.devflowai.domain.TenantQuota;
import com.devflowai.event.JobEvent;
import com.devflowai.job.JobPriority;
import com.devflowai.job.JobStatus;
import com.devflowai.repository.JobRepository;
import com.devflowai.repository.OutboxEventRepository;
import com.devflowai.repository.TenantQuotaRepository;
import com.devflowai.service.JobExecutionContext;
import com.devflowai.service.JobExecutionService;
import com.devflowai.service.JobHandler;
import com.devflowai.service.OutboxDispatchService;
import com.devflowai.service.JobService;
import com.devflowai.service.RetryPolicy;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.MapPropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@SpringBootTest(
        classes = {DevFlowAiApplication.class, WorkerProcessRecoveryIntegrationTest.TestHandlers.class},
        properties = {
                "devflow.scheduling.max-concurrent-jobs=500",
                "devflow.scheduling.max-concurrent-per-tenant=50",
                "devflow.worker.claim-batch-size=100",
                "devflow.worker.lease-duration-seconds=120",
                "devflow.worker.lease-heartbeat-ms=3600000",
                "devflow.worker.recovery-interval-ms=3600000",
                "devflow.outbox.poll-interval-ms=3600000",
                "devflow.kafka.publish-timeout-seconds=3",
                "spring.kafka.producer.properties.max.block.ms=5000"
        })
@ActiveProfiles({"production", "kafka"})
class WorkerProcessRecoveryIntegrationTest {
    private static final String TOPIC = "devflow-process-recovery-it";

    @Container
    private static final MySQLContainer MYSQL = new MySQLContainer(DockerImageName.parse("mysql:8.4"))
            .withDatabaseName("devflow_process_it")
            .withUsername("devflow")
            .withPassword("devflow-test-only");
    @Container
    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    @Autowired
    private JobService jobService;
    @Autowired
    private JobExecutionService executionService;
    @Autowired
    private JobRepository jobRepository;
    @Autowired
    private TenantQuotaRepository quotaRepository;
    @Autowired
    private OutboxDispatchService outboxDispatchService;
    @Autowired
    private OutboxEventRepository outboxRepository;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;
    private final List<Long> ownedJobs = new ArrayList<>();
    private final Map<Long, String> ownedTenants = new HashMap<>();

    @DynamicPropertySource
    static void configureInfrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.datasource.driver-class-name", MYSQL::getDriverClassName);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.consumer.group-id", () -> "devflow-process-recovery-workers");
        registry.add("devflow.kafka.job-events-topic", () -> TOPIC);
    }

    @BeforeEach
    void createTopic() throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(properties)) {
            try {
                admin.createTopics(List.of(new NewTopic(TOPIC, 2, (short) 1)))
                        .all().get(10, TimeUnit.SECONDS);
            } catch (ExecutionException exception) {
                if (!(exception.getCause() instanceof TopicExistsException)) {
                    throw exception;
                }
            }
        }
    }

    @AfterEach
    void removeOnlyRowsCreatedByThisTest() {
        for (Long jobId : ownedJobs) {
            jdbcTemplate.update("DELETE FROM outbox_events WHERE job_id = ?", jobId);
            jdbcTemplate.update("DELETE FROM job_execution_history WHERE job_id = ?", jobId);
            jdbcTemplate.update("DELETE FROM jobs WHERE id = ?", jobId);
            jdbcTemplate.update("DELETE FROM tenant_quotas WHERE tenant_id = ?", ownedTenants.get(jobId));
            BlockingExecutionHandler.clear(jobId);
        }
        ownedJobs.clear();
        ownedTenants.clear();
    }

    @Test
    void forciblyTerminatedWorkerIsRecoveredByReplacementApplicationInstance() throws Exception {
        var submission = submit("process-crash-it", "process-crash");
        JobEvent originalEvent = eventFor(submission.getId());
        Path marker = markerPath();
        ChildProcess child = startProcess("worker", submission.getId(), originalEvent.tenantId(),
                originalEvent.correlationId(), marker);
        String staleToken;
        try {
            awaitMarker(marker, child);
            JobEntity running = jobRepository.findById(submission.getId()).orElseThrow();
            assertEquals(JobStatus.RUNNING, running.getStatus());
            staleToken = running.getLeaseToken();
            assertNotNull(staleToken);
            child.process().destroyForcibly();
            assertTrue(child.process().waitFor(10, TimeUnit.SECONDS), "worker process did not terminate");
        } finally {
            if (child.process().isAlive()) {
                child.process().destroyForcibly();
                child.process().waitFor(10, TimeUnit.SECONDS);
            }
            child.outputReader().join(TimeUnit.SECONDS.toMillis(5));
            Files.deleteIfExists(marker);
        }

        assertEquals(JobStatus.RUNNING, jobRepository.findById(submission.getId()).orElseThrow().getStatus());
        assertEquals(1, jdbcTemplate.update("""
                UPDATE jobs SET lease_expires_at = DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 SECOND)
                WHERE id = ? AND status = 'RUNNING'
                """, submission.getId()));

        try (ConfigurableApplicationContext replacement = startIndependentInstance(false)) {
            JobExecutionService replacementService = replacement.getBean(JobExecutionService.class);
            assertEquals(1, replacementService.recoverExpiredLeases());
            assertEquals(JobStatus.RETRY_SCHEDULED,
                    jobRepository.findById(submission.getId()).orElseThrow().getStatus());
            makeRetryDue(submission.getId());
            assertEquals(1, replacementService.scheduleDueRetries());
            replacementService.consume(latestEvent(submission.getId()));
        }

        JobEntity completed = jobRepository.findById(submission.getId()).orElseThrow();
        assertEquals(JobStatus.SUCCEEDED, completed.getStatus());
        assertEquals(2, completed.getAttemptCount());
        assertEquals("process-crash", completed.getResult());
        assertFalse(completed.complete(staleToken, "stale process result", Instant.now()));
        assertEquals("process-crash", jobRepository.findById(submission.getId()).orElseThrow().getResult());
    }

    @Test
    void killedDispatcherAfterRealBrokerAcknowledgementReclaimsOutboxAndDuplicateDeliveryIsIdempotent()
            throws Exception {
        var submission = submit("echo", "outbox-process-crash");
        OutboxEvent pending = outboxFor(submission.getId());
        JobEvent event = objectMapper.readValue(pending.getPayload(), JobEvent.class);
        Path marker = markerPath();
        ChildProcess child = startProcess("outbox", submission.getId(), event.tenantId(),
                event.correlationId(), marker);
        try {
            awaitMarker(marker, child);
            OutboxEvent inFlight = outboxRepository.findById(pending.getEventId()).orElseThrow();
            assertEquals(OutboxEvent.Status.IN_FLIGHT, inFlight.getStatus());
            assertEquals(1, inFlight.getAttemptCount());
            child.process().destroyForcibly();
            assertTrue(child.process().waitFor(10, TimeUnit.SECONDS), "dispatcher process did not terminate");
        } finally {
            if (child.process().isAlive()) {
                child.process().destroyForcibly();
                child.process().waitFor(10, TimeUnit.SECONDS);
            }
            child.outputReader().join(TimeUnit.SECONDS.toMillis(5));
            Files.deleteIfExists(marker);
        }

        assertEquals(1, jdbcTemplate.update("""
                UPDATE outbox_events SET lease_expires_at = DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 SECOND)
                WHERE event_id = ? AND status = 'IN_FLIGHT'
                """, pending.getEventId()));
        assertTrue(outboxDispatchService.dispatchAvailable() >= 1);
        assertEquals(2, brokerRecordsForEvent(event.eventId()));

        executionService.consume(event);
        executionService.consume(event);
        JobEntity persisted = jobRepository.findById(submission.getId()).orElseThrow();
        assertEquals(JobStatus.SUCCEEDED, persisted.getStatus());
        assertEquals(1, persisted.getAttemptCount());
        assertEquals(OutboxEvent.Status.DELIVERED,
                outboxRepository.findById(event.eventId()).orElseThrow().getStatus());
    }

    @Test
    void staleWorkerCompletionIsFencedAfterReplacementCompletesRetry() throws Exception {
        var submission = submit("stale-worker-it", "stale-worker");
        BlockingExecutionHandler.arm(submission.getId());
        JobEvent event = eventFor(submission.getId());
        ExecutorService worker = Executors.newSingleThreadExecutor();
        Future<?> staleExecution = worker.submit(() -> executionService.consume(event));
        try {
            assertTrue(BlockingExecutionHandler.awaitFirstClaim(submission.getId(), 10, TimeUnit.SECONDS),
                    "first worker did not enter its handler");
            String staleToken = jobRepository.findById(submission.getId()).orElseThrow().getLeaseToken();
            assertEquals(1, jdbcTemplate.update("""
                    UPDATE jobs SET lease_expires_at = DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 SECOND)
                    WHERE id = ? AND status = 'RUNNING'
                    """, submission.getId()));

            try (ConfigurableApplicationContext replacement = startIndependentInstance(false)) {
                JobExecutionService replacementService = replacement.getBean(JobExecutionService.class);
                assertEquals(1, replacementService.recoverExpiredLeases());
                makeRetryDue(submission.getId());
                assertEquals(1, replacementService.scheduleDueRetries());
                replacementService.consume(latestEvent(submission.getId()));
            }
            BlockingExecutionHandler.releaseFirst(submission.getId());
            staleExecution.get(15, TimeUnit.SECONDS);

            JobEntity completed = jobRepository.findById(submission.getId()).orElseThrow();
            assertEquals(JobStatus.SUCCEEDED, completed.getStatus());
            assertEquals(2, completed.getAttemptCount());
            assertEquals("replacement-result", completed.getResult());
            assertFalse(completed.complete(staleToken, "stale result", Instant.now()));
        } finally {
            BlockingExecutionHandler.releaseFirst(submission.getId());
            worker.shutdownNow();
            worker.awaitTermination(5, TimeUnit.SECONDS);
            BlockingExecutionHandler.clear(submission.getId());
        }
    }

    @Test
    void unrelatedKafkaEventCannotClaimDueRetryBeforeTransactionalOutboxScheduling() throws Exception {
        Awaitility.await().atMost(15, TimeUnit.SECONDS).until(this::workerConsumerGroupCaughtUp);

        JobResponse retryJob = submit("contention-retry-it", "retry-race", 2);
        JobEvent originalRetryEvent = eventFor(retryJob.getId());
        executionService.consume(originalRetryEvent);
        assertEquals(JobStatus.RETRY_SCHEDULED,
                jobRepository.findById(retryJob.getId()).orElseThrow().getStatus());
        deferRetry(retryJob.getId());
        outboxDispatchService.dispatchAvailable();
        Awaitility.await().atMost(15, TimeUnit.SECONDS).until(this::workerConsumerGroupCaughtUp);
        assertEquals(1, outboxCount(retryJob.getId()));
        makeRetryDue(retryJob.getId());

        JobResponse unrelatedJob = submit("echo", "unrelated-event", 2);
        JobEvent unrelatedEvent = eventFor(unrelatedJob.getId());
        kafkaTemplate.send(TOPIC, unrelatedEvent.eventId(), unrelatedEvent)
                .get(10, TimeUnit.SECONDS);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(JobStatus.SUCCEEDED,
                        jobRepository.findById(unrelatedJob.getId()).orElseThrow().getStatus()));

        JobEntity dueRetry = jobRepository.findById(retryJob.getId()).orElseThrow();
        assertEquals(JobStatus.RETRY_SCHEDULED, dueRetry.getStatus());
        assertEquals(1, dueRetry.getAttemptCount());
        assertEquals(1, outboxCount(retryJob.getId()));

        try (ConfigurableApplicationContext second = startIndependentInstance(false)) {
            JobExecutionService secondExecutionService = second.getBean(JobExecutionService.class);
            CountDownLatch scheduleStart = new CountDownLatch(1);
            ExecutorService schedulers = Executors.newFixedThreadPool(2);
            try {
                Future<Integer> first = schedulers.submit(() -> {
                    scheduleStart.await();
                    return executionService.scheduleDueRetries();
                });
                Future<Integer> secondSchedule = schedulers.submit(() -> {
                    scheduleStart.await();
                    return secondExecutionService.scheduleDueRetries();
                });
                scheduleStart.countDown();
                assertEquals(1, first.get(20, TimeUnit.SECONDS)
                        + secondSchedule.get(20, TimeUnit.SECONDS));
            } finally {
                schedulers.shutdownNow();
            }

            JobEntity queuedRetry = jobRepository.findById(retryJob.getId()).orElseThrow();
            assertEquals(JobStatus.QUEUED, queuedRetry.getStatus());
            assertEquals(2, outboxCount(retryJob.getId()));
            JobEvent retryEvent = eventAfter(retryJob.getId(), originalRetryEvent.eventId());

            CountDownLatch claimStart = new CountDownLatch(1);
            ExecutorService claimers = Executors.newFixedThreadPool(2);
            try {
                Future<?> first = claimers.submit(() -> {
                    await(claimStart);
                    executionService.consume(retryEvent);
                });
                Future<?> secondClaim = claimers.submit(() -> {
                    await(claimStart);
                    secondExecutionService.consume(retryEvent);
                });
                claimStart.countDown();
                first.get(20, TimeUnit.SECONDS);
                secondClaim.get(20, TimeUnit.SECONDS);
            } finally {
                claimers.shutdownNow();
            }
        }

        JobEntity completedRetry = jobRepository.findById(retryJob.getId()).orElseThrow();
        assertEquals(JobStatus.SUCCEEDED, completedRetry.getStatus());
        assertEquals(2, completedRetry.getAttemptCount());
        assertEquals(2, BlockingExecutionHandler.invocations(retryJob.getId()));

        JobEvent retryEvent = latestEvent(retryJob.getId());
        outboxDispatchService.dispatchAvailable();
        kafkaTemplate.send(TOPIC, retryEvent.eventId(), retryEvent).get(10, TimeUnit.SECONDS);
        kafkaTemplate.send(TOPIC, retryEvent.eventId(), retryEvent).get(10, TimeUnit.SECONDS);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(this::workerConsumerGroupCaughtUp);
        assertEquals(JobStatus.SUCCEEDED, jobRepository.findById(retryJob.getId()).orElseThrow().getStatus());
        assertEquals(2, jobRepository.findById(retryJob.getId()).orElseThrow().getAttemptCount());
        assertEquals(2, BlockingExecutionHandler.invocations(retryJob.getId()));

        JobResponse exhaustedJob = submit("retry-exhaustion-it", "always-fail", 2);
        JobEvent originalExhaustionEvent = eventFor(exhaustedJob.getId());
        executionService.consume(originalExhaustionEvent);
        assertEquals(JobStatus.RETRY_SCHEDULED,
                jobRepository.findById(exhaustedJob.getId()).orElseThrow().getStatus());
        deferRetry(exhaustedJob.getId());
        outboxDispatchService.dispatchAvailable();
        Awaitility.await().atMost(15, TimeUnit.SECONDS).until(this::workerConsumerGroupCaughtUp);
        makeRetryDue(exhaustedJob.getId());
        assertEquals(1, executionService.scheduleDueRetries());
        assertEquals(2, outboxCount(exhaustedJob.getId()));
        JobEvent exhaustedRetryEvent = eventAfter(exhaustedJob.getId(), originalExhaustionEvent.eventId());
        outboxDispatchService.dispatchAvailable();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(OutboxEvent.Status.DELIVERED,
                        outboxRepository.findById(exhaustedRetryEvent.eventId()).orElseThrow().getStatus()));
        executionService.consume(exhaustedRetryEvent);
        assertEquals(JobStatus.DEAD_LETTER,
                jobRepository.findById(exhaustedJob.getId()).orElseThrow().getStatus());
        assertEquals(2, jobRepository.findById(exhaustedJob.getId()).orElseThrow().getAttemptCount());
        assertEquals(2, BlockingExecutionHandler.invocations(exhaustedJob.getId()));
    }

    @Test
    void twoInstancesSustainConcurrentSubmissionsClaimsRetriesAndDuplicateKafkaEvents() throws Exception {
        long started = System.nanoTime();
        try (ConfigurableApplicationContext second = startIndependentInstance(true)) {
            JobService secondJobService = second.getBean(JobService.class);
            JobExecutionService secondExecutionService = second.getBean(JobExecutionService.class);
            OutboxDispatchService secondDispatcher = second.getBean(OutboxDispatchService.class);
            List<JobResponse> submissions = new ArrayList<>();
            List<String> tenants = new ArrayList<>();
            ExecutorService submitters = Executors.newFixedThreadPool(8);
            try {
                List<Future<Submission>> futures = new ArrayList<>();
                for (int tenantIndex = 0; tenantIndex < 12; tenantIndex++) {
                    String tenant = "stress-" + UUID.randomUUID();
                    quotaRepository.saveAndFlush(new TenantQuota(tenant, 5, 50));
                    for (int jobIndex = 0; jobIndex < 4; jobIndex++) {
                        if (tenantIndex == 0 && jobIndex == 0) {
                            continue;
                        }
                        JobService target = (tenantIndex + jobIndex) % 2 == 0 ? jobService : secondJobService;
                        futures.add(submitters.submit(() -> new Submission(tenant,
                                target.submitJob(tenant, new JobSubmissionRequest(
                                        "echo", "stress-payload",
                                        JobPriority.NORMAL, 1, 3, UUID.randomUUID().toString())))));
                    }
                }
                for (Future<Submission> future : futures) {
                    Submission submitted = future.get(60, TimeUnit.SECONDS);
                    submissions.add(submitted.response());
                    tenants.add(submitted.tenant());
                    ownedJobs.add(submitted.response().getId());
                    ownedTenants.put(submitted.response().getId(), submitted.tenant());
                }
            } finally {
                submitters.shutdownNow();
                submitters.awaitTermination(5, TimeUnit.SECONDS);
            }

            List<JobEvent> duplicateEvents = submissions.stream()
                    .limit(12)
                    .map(response -> eventFor(response.getId()))
                    .toList();
            outboxDispatchService.dispatchAvailable();
            secondDispatcher.dispatchAvailable();

            Awaitility.await().atMost(45, TimeUnit.SECONDS).untilAsserted(() -> {
                for (int i = 0; i < submissions.size(); i++) {
                    JobEntity entity = jobRepository.findById(submissions.get(i).getId()).orElseThrow();
                    assertEquals(JobStatus.SUCCEEDED, entity.getStatus());
                }
            });
            Awaitility.await().atMost(15, TimeUnit.SECONDS).until(this::workerConsumerGroupCaughtUp);

            JobResponse tenantA = submissions.get(1);
            JobEvent forgedTenantEvent = new JobEvent(UUID.randomUUID().toString(), JobEvent.TYPE,
                    JobEvent.CURRENT_SCHEMA_VERSION, tenantA.getId(), "not-" + tenants.get(1),
                    tenantA.getCorrelationId(), Instant.now());
            assertThrows(IllegalArgumentException.class, () -> executionService.consume(forgedTenantEvent));
            assertEquals(JobStatus.SUCCEEDED,
                    jobRepository.findById(tenantA.getId()).orElseThrow().getStatus());

            String retryTenant = tenants.get(0);
            JobResponse retrySubmission = jobService.submitJob(retryTenant, new JobSubmissionRequest(
                    "contention-retry-it", "retry-once", JobPriority.NORMAL, 1, 3,
                    UUID.randomUUID().toString()));
            submissions.add(retrySubmission);
            tenants.add(retryTenant);
            ownedJobs.add(retrySubmission.getId());
            ownedTenants.put(retrySubmission.getId(), retryTenant);
            executionService.consume(eventFor(retrySubmission.getId()));
            assertEquals(JobStatus.RETRY_SCHEDULED,
                    jobRepository.findById(retrySubmission.getId()).orElseThrow().getStatus());
            makeRetryDue(retrySubmission.getId());
            CountDownLatch retryStart = new CountDownLatch(1);
            ExecutorService retrySchedulers = Executors.newFixedThreadPool(2);
            try {
                Future<Integer> first = retrySchedulers.submit(() -> {
                    retryStart.await();
                    return executionService.scheduleDueRetries();
                });
                Future<Integer> secondSchedule = retrySchedulers.submit(() -> {
                    retryStart.await();
                    return secondExecutionService.scheduleDueRetries();
                });
                retryStart.countDown();
                assertEquals(1, first.get(20, TimeUnit.SECONDS) + secondSchedule.get(20, TimeUnit.SECONDS));
            } finally {
                retrySchedulers.shutdownNow();
            }
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                assertEquals(2, jdbcTemplate.queryForObject(
                        "SELECT COUNT(*) FROM outbox_events WHERE job_id = ?", Integer.class,
                        retrySubmission.getId()));
            });

            JobEvent retryEvent = eventAfter(
                    retrySubmission.getId(), eventFor(retrySubmission.getId()).eventId());
            outboxDispatchService.dispatchAvailable();
            Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(OutboxEvent.Status.DELIVERED,
                            outboxRepository.findById(retryEvent.eventId()).orElseThrow().getStatus()));
            Awaitility.await().atMost(15, TimeUnit.SECONDS).until(this::workerConsumerGroupCaughtUp);

            Awaitility.await().atMost(45, TimeUnit.SECONDS).untilAsserted(() -> {
                for (int i = 0; i < submissions.size(); i++) {
                    JobEntity entity = jobRepository.findById(submissions.get(i).getId()).orElseThrow();
                    String tenant = tenants.get(i);
                    assertEquals(JobStatus.SUCCEEDED, entity.getStatus());
                    assertEquals(entity.getId().equals(retrySubmission.getId()) ? 2 : 1,
                            entity.getAttemptCount());
                    assertEquals(0, jobRepository.countByTenantIdAndStatusIn(tenant,
                                    List.of(JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.RETRY_SCHEDULED)),
                            () -> "Unfinished jobs for tenant " + tenant + ": "
                                    + jdbcTemplate.queryForList(
                                    "SELECT id, status, job_type FROM jobs WHERE tenant_id = ?",
                                    tenant));
                }
            });
            assertEquals(2, BlockingExecutionHandler.invocations(retrySubmission.getId()));
            for (JobEvent duplicate : duplicateEvents) {
                kafkaTemplate.send(TOPIC, duplicate.eventId(), duplicate).get(10, TimeUnit.SECONDS);
                kafkaTemplate.send(TOPIC, duplicate.eventId(), duplicate).get(10, TimeUnit.SECONDS);
            }
            Awaitility.await().atMost(15, TimeUnit.SECONDS).until(this::workerConsumerGroupCaughtUp);
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        System.out.println("MULTI_INSTANCE_STRESS instances=2 jobs=48 tenants=12 duplicateEvents=24 "
                + "retryJobs=1 elapsed_ms=" + elapsedMillis + " mysql=mysql:8.4 kafka=3.8.0");
    }

    private ChildProcess startProcess(String mode, Long jobId, String tenant, String correlationId,
                                      Path marker) throws IOException {
        String classPath = System.getProperty("surefire.test.class.path",
                System.getProperty("java.class.path"));
        Path java = Path.of(System.getProperty("java.home"), "bin", "java.exe");
        Process process = new ProcessBuilder(java.toString(), "-cp", classPath, WorkerProcessHarness.class.getName(),
                mode, MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword(),
                KAFKA.getBootstrapServers(), TOPIC, jobId.toString(), tenant, correlationId, marker.toString())
                .redirectErrorStream(true)
                .start();
        StringBuilder output = new StringBuilder();
        Thread outputReader = new Thread(() -> copyOutput(process.getInputStream(), output),
                "worker-process-output-" + process.pid());
        outputReader.setDaemon(true);
        outputReader.start();
        return new ChildProcess(process, output, outputReader);
    }

    private void awaitMarker(Path marker, ChildProcess child) {
        Awaitility.await().atMost(Duration.ofSeconds(45)).pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    if (!child.process().isAlive()) {
                        throw new AssertionError("worker process exited early: "
                                + child.output());
                    }
                    assertTrue(Files.exists(marker), "child did not reach its deterministic crash point");
                });
    }

    private static void copyOutput(InputStream stream, StringBuilder output) {
        try (stream) {
            byte[] buffer = new byte[1024];
            int count;
            while ((count = stream.read(buffer)) >= 0) {
                synchronized (output) {
                    if (output.length() < 16_384) {
                        output.append(new String(buffer, 0, count, java.nio.charset.StandardCharsets.UTF_8));
                    }
                }
            }
        } catch (IOException exception) {
            synchronized (output) {
                output.append("\n[output capture failed: ").append(exception.getMessage()).append(']');
            }
        }
    }

    private record ChildProcess(Process process, StringBuilder capturedOutput, Thread outputReader) {
        private String output() {
            synchronized (capturedOutput) {
                return capturedOutput.toString();
            }
        }
    }

    private ConfigurableApplicationContext startIndependentInstance(boolean withKafka) {
        Map<String, Object> settings = new HashMap<>();
        settings.put("spring.datasource.url", MYSQL.getJdbcUrl());
        settings.put("spring.datasource.username", MYSQL.getUsername());
        settings.put("spring.datasource.password", MYSQL.getPassword());
        settings.put("spring.datasource.driver-class-name", MYSQL.getDriverClassName());
        settings.put("spring.flyway.enabled", "false");
        settings.put("spring.jpa.hibernate.ddl-auto", "none");
        settings.put("spring.kafka.bootstrap-servers", KAFKA.getBootstrapServers());
        settings.put("spring.kafka.consumer.group-id", "devflow-process-recovery-workers");
        settings.put("devflow.kafka.job-events-topic", TOPIC);
        settings.put("devflow.worker.claim-batch-size", "100");
        settings.put("devflow.worker.lease-duration-seconds", "120");
        settings.put("devflow.worker.lease-heartbeat-ms", "3600000");
        settings.put("devflow.worker.recovery-interval-ms", "3600000");
        settings.put("devflow.outbox.poll-interval-ms", "3600000");
        settings.put("spring.main.web-application-type", "none");
        SpringApplicationBuilder builder = new SpringApplicationBuilder(
                DevFlowAiApplication.class, TestHandlers.class)
                .profiles(withKafka ? new String[]{"production", "kafka"} : new String[]{"production"})
                .web(WebApplicationType.NONE)
                .initializers(context -> context.getEnvironment().getPropertySources()
                        .addFirst(new MapPropertySource("independent-worker-instance", settings)));
        return builder.run();
    }

    private JobResponse submit(String type, String payload) {
        return submit(type, payload, 3);
    }

    private JobResponse submit(String type, String payload, int maxAttempts) {
        JobSubmissionRequest request = new JobSubmissionRequest(type, payload, JobPriority.NORMAL,
                1, maxAttempts, UUID.randomUUID().toString());
        String tenant = "process-it-" + UUID.randomUUID();
        JobResponse response = jobService.submitJob(tenant, request);
        ownedJobs.add(response.getId());
        ownedTenants.put(response.getId(), tenant);
        return response;
    }

    private int outboxCount(Long jobId) {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE job_id = ?", Integer.class, jobId);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Concurrent worker start barrier timed out");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for worker start barrier", exception);
        }
    }

    private JobEvent eventFor(Long jobId) {
        OutboxEvent outbox = outboxFor(jobId);
        return new JobEvent(outbox.getEventId(), JobEvent.TYPE, JobEvent.CURRENT_SCHEMA_VERSION,
                jobId, jobRepository.findById(jobId).orElseThrow().getTenantId(),
                outbox.getCorrelationId(), Instant.now());
    }

    private OutboxEvent outboxFor(Long jobId) {
        return outboxRepository.findAll().stream()
                .filter(row -> row.getJobId().equals(jobId))
                .min(Comparator.comparing(OutboxEvent::getCreatedAt))
                .orElseThrow();
    }

    private JobEvent latestEvent(Long jobId) throws IOException {
        OutboxEvent latest = outboxRepository.findAll().stream()
                .filter(row -> row.getJobId().equals(jobId))
                .max(Comparator.comparing(OutboxEvent::getCreatedAt))
                .orElseThrow();
        return objectMapper.readValue(latest.getPayload(), JobEvent.class);
    }

    private JobEvent eventAfter(Long jobId, String previousEventId) throws IOException {
        OutboxEvent next = outboxRepository.findAll().stream()
                .filter(row -> row.getJobId().equals(jobId))
                .filter(row -> !row.getEventId().equals(previousEventId))
                .findFirst()
                .orElseThrow();
        return objectMapper.readValue(next.getPayload(), JobEvent.class);
    }

    private void makeRetryDue(Long jobId) {
        assertEquals(1, jdbcTemplate.update("""
                UPDATE jobs SET scheduled_at = '2000-01-01 00:00:00'
                WHERE id = ? AND status = 'RETRY_SCHEDULED'
                """, jobId));
    }

    private void deferRetry(Long jobId) {
        assertEquals(1, jdbcTemplate.update("""
                UPDATE jobs SET scheduled_at = '2037-01-01 00:00:00'
                WHERE id = ? AND status = 'RETRY_SCHEDULED'
                """, jobId));
    }

    private boolean workerConsumerGroupCaughtUp() throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(properties)) {
            var topic = admin.describeTopics(List.of(TOPIC)).allTopicNames().get(10, TimeUnit.SECONDS).get(TOPIC);
            List<TopicPartition> partitions = topic.partitions().stream()
                    .map(partition -> new TopicPartition(TOPIC, partition.partition()))
                    .toList();
            var committed = admin.listConsumerGroupOffsets("devflow-process-recovery-workers")
                    .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
            var latest = admin.listOffsets(partitions.stream().collect(
                    java.util.stream.Collectors.toMap(partition -> partition, ignored -> OffsetSpec.latest())))
                    .all().get(10, TimeUnit.SECONDS);
            return partitions.stream().allMatch(partition -> {
                long committedOffset = committed.get(partition) == null
                        ? 0 : committed.get(partition).offset();
                return committedOffset >= latest.get(partition).offset();
            });
        }
    }

    private record Submission(String tenant, JobResponse response) {
    }

    private Path markerPath() throws IOException {
        Path directory = Path.of("target", "worker-process-test");
        Files.createDirectories(directory);
        return directory.resolve(UUID.randomUUID() + ".ready");
    }

    private int brokerRecordsForEvent(String eventId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "process-event-check-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        int matches = 0;
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(TOPIC));
            while (System.nanoTime() < deadline && matches < 2) {
                for (var record : consumer.poll(Duration.ofMillis(250))) {
                    if (record.value() != null && record.value().contains(eventId)) {
                        matches++;
                    }
                }
            }
        }
        return matches;
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class TestHandlers {
        @Bean
        @org.springframework.context.annotation.Primary
        RetryPolicy deterministicRetryPolicy() {
            return new RetryPolicy() {
                @Override
                public Duration delayForAttempt(int attempt) {
                    return Duration.ofMinutes(5);
                }
            };
        }

        @Bean
        @Order(Ordered.HIGHEST_PRECEDENCE)
        JobHandler recoveryTestHandler() {
            return new BlockingExecutionHandler();
        }
    }

    static final class BlockingExecutionHandler implements JobHandler {
        private static final ConcurrentHashMap<Long, Attempt> ATTEMPTS = new ConcurrentHashMap<>();

        static void arm(Long jobId) {
            ATTEMPTS.put(jobId, new Attempt());
        }

        static boolean awaitFirstClaim(Long jobId, long timeout, TimeUnit unit) throws InterruptedException {
            Attempt attempt = ATTEMPTS.get(jobId);
            return attempt != null && attempt.firstStarted.await(timeout, unit);
        }

        static void releaseFirst(Long jobId) {
            Attempt attempt = ATTEMPTS.get(jobId);
            if (attempt != null) {
                attempt.releaseFirst.countDown();
            }
        }

        static void clear(Long jobId) {
            ATTEMPTS.remove(jobId);
        }

        @Override
        public boolean supports(String jobType) {
            return "stale-worker-it".equals(jobType) || "contention-retry-it".equals(jobType)
                    || "retry-exhaustion-it".equals(jobType);
        }

        @Override
        public String execute(JobExecutionContext context) throws Exception {
            Attempt attempt = ATTEMPTS.computeIfAbsent(context.jobId(), ignored -> new Attempt());
            int invocation = attempt.invocations.incrementAndGet();
            if ("contention-retry-it".equals(context.jobType())) {
                if (invocation == 1) {
                    throw new IllegalStateException("deterministic first-attempt failure");
                }
                return "retry-succeeded";
            }
            if ("retry-exhaustion-it".equals(context.jobType())) {
                throw new IllegalStateException("deterministic retry exhaustion");
            }
            if (invocation == 1) {
                attempt.firstStarted.countDown();
                if (!attempt.releaseFirst.await(30, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("stale-worker test gate timed out");
                }
                return "stale-result";
            }
            return "replacement-result";
        }

        static int invocations(Long jobId) {
            Attempt attempt = ATTEMPTS.get(jobId);
            return attempt == null ? 0 : attempt.invocations.get();
        }

        private static final class Attempt {
            private final AtomicInteger invocations = new AtomicInteger();
            private final CountDownLatch firstStarted = new CountDownLatch(1);
            private final CountDownLatch releaseFirst = new CountDownLatch(1);
        }
    }
}
