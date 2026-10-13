package com.devflowai.integration;

import com.devflowai.api.dto.JobSubmissionRequest;
import com.devflowai.api.dto.JobResponse;
import com.devflowai.DevFlowAiApplication;
import com.devflowai.domain.JobEntity;
import com.devflowai.domain.OutboxEvent;
import com.devflowai.domain.TenantQuota;
import com.devflowai.event.JobEvent;
import com.devflowai.job.JobPriority;
import com.devflowai.job.JobStatus;
import com.devflowai.repository.JobRepository;
import com.devflowai.repository.OutboxEventRepository;
import com.devflowai.repository.TenantQuotaRepository;
import com.devflowai.service.JobExecutionService;
import com.devflowai.service.JobService;
import com.devflowai.service.OutboxDispatchService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.testcontainers.DockerClientFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
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

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@SpringBootTest(properties = {
        "devflow.scheduling.max-concurrent-jobs=10",
        "devflow.scheduling.max-concurrent-per-tenant=10",
        "devflow.worker.claim-batch-size=10",
        "devflow.worker.recovery-interval-ms=3600000",
        "devflow.worker.lease-heartbeat-ms=3600000",
        "devflow.outbox.poll-interval-ms=3600000",
        "devflow.kafka.publish-timeout-seconds=2",
        "spring.kafka.producer.properties.max.block.ms=5000"
})
@ActiveProfiles({"production", "kafka"})
class OutboxKafkaMySqlIntegrationTest {
    private static final String JOB_EVENTS_TOPIC = "devflow.outbox-mysql-it";

    @Container
    private static final MySQLContainer MYSQL =
            new MySQLContainer(DockerImageName.parse("mysql:8.4"))
                    .withDatabaseName("devflow_outbox_it")
                    .withUsername("devflow")
                    .withPassword("devflow-test-only");
    @Container
    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    @Autowired
    private JobService jobService;
    @Autowired
    private JobRepository jobRepository;
    @Autowired
    private TenantQuotaRepository quotaRepository;
    @Autowired
    private JobExecutionService executionService;
    @Autowired
    private OutboxEventRepository outboxRepository;
    @Autowired
    private OutboxDispatchService dispatcher;
    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private MeterRegistry meterRegistry;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @DynamicPropertySource
    static void configureInfrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.datasource.driver-class-name", MYSQL::getDriverClassName);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.consumer.group-id", () -> "devflow-outbox-it-workers");
        registry.add("devflow.kafka.job-events-topic", () -> JOB_EVENTS_TOPIC);
    }

    @BeforeEach
    void ensureKafkaIsRunning() throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(properties)) {
            try {
                admin.createTopics(List.of(new NewTopic(JOB_EVENTS_TOPIC, 1, (short) 1)))
                        .all().get(10, TimeUnit.SECONDS);
            } catch (ExecutionException ex) {
                if (!(ex.getCause() instanceof TopicExistsException)) {
                    throw ex;
                }
            }
        }
    }

    @Test
    void databaseOutboxIsMarkedDeliveredAfterRealKafkaAcknowledgementAndWorkerExecution() {
        var submission = submitJob("outbox-happy");
        OutboxEvent pending = outboxFor(submission.getId());
        assertEquals(OutboxEvent.Status.PENDING, pending.getStatus());
        double publishedBefore = meterRegistry.counter("devflow.outbox.published").count();

        dispatcher.dispatchAvailable();

        Awaitility.await().atMost(20, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals("SUCCEEDED", jobRepository.findById(submission.getId())
                        .orElseThrow().getStatus().name()));
        OutboxEvent delivered = outboxRepository.findById(pending.getEventId()).orElseThrow();
        assertEquals(OutboxEvent.Status.DELIVERED, delivered.getStatus());
        assertEquals(1, delivered.getAttemptCount());
        assertEquals(Instant.class, delivered.getPublishedAt().getClass());
        assertTrue(meterRegistry.counter("devflow.outbox.published").count() >= publishedBefore + 1);
        assertTrue(meterRegistry.timer("devflow.worker.job.processing", "outcome", "succeeded").count() > 0);
        assertFalse(meterRegistry.getMeters().stream()
                .flatMap(meter -> meter.getId().getTags().stream())
                .anyMatch(tag -> tag.getValue().equals(submission.getId().toString())
                        || tag.getValue().equals(pending.getEventId())
                        || tag.getValue().equals(pending.getCorrelationId())));
    }

    @Test
    void brokerOutageLeavesRetryableOutboxAndPublisherRecoversWhenBrokerReturns() {
        double failuresBefore = meterRegistry.counter("devflow.outbox.failed").count();
        DockerClientFactory.instance().client().pauseContainerCmd(KAFKA.getContainerId()).exec();
        JobResponse submission;
        OutboxEvent pending;
        try {
            submission = submitJob("outbox-recovery");
            pending = outboxFor(submission.getId());
            dispatcher.dispatchAvailable();
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                OutboxEvent attempt = outboxRepository.findById(pending.getEventId()).orElseThrow();
                assertEquals(1, attempt.getAttemptCount());
                assertEquals(OutboxEvent.Status.PENDING, attempt.getStatus());
            });
            OutboxEvent failed = outboxRepository.findById(pending.getEventId()).orElseThrow();
            assertEquals(OutboxEvent.Status.PENDING, failed.getStatus());
            assertEquals(1, failed.getAttemptCount());
            assertFalse(failed.getLastError().isBlank());
            assertTrue(failed.getNextAttemptAt().isAfter(Instant.now()));
        } finally {
            DockerClientFactory.instance().client().unpauseContainerCmd(KAFKA.getContainerId()).exec();
        }
        Awaitility.await().ignoreExceptions().atMost(10, TimeUnit.SECONDS).until(() ->
                !kafkaTemplate.partitionsFor(JOB_EVENTS_TOPIC).isEmpty());
        Awaitility.await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            dispatcher.dispatchAvailable();
            assertEquals(OutboxEvent.Status.DELIVERED,
                    outboxRepository.findById(pending.getEventId()).orElseThrow().getStatus());
            assertEquals("SUCCEEDED", jobRepository.findById(submission.getId())
                    .orElseThrow().getStatus().name());
        });
        OutboxEvent recovered = outboxRepository.findById(pending.getEventId()).orElseThrow();
        assertEquals(OutboxEvent.Status.DELIVERED, recovered.getStatus());
        assertTrue(recovered.getAttemptCount() >= 2);
        assertTrue(meterRegistry.counter("devflow.outbox.failed").count() > failuresBefore);
    }

    @Test
    void brokerAcknowledgementBeforeOutboxUpdateCanRepublishSameEventWithoutDuplicateExecution()
            throws Exception {
        var submission = submitJob("outbox-duplicate");
        OutboxEvent pending = outboxFor(submission.getId());
        JobEvent event = objectMapper.readValue(pending.getPayload(), JobEvent.class);

        pending.claim("simulated-crashed-dispatcher", Instant.now().plusSeconds(30));
        outboxRepository.saveAndFlush(pending);
        kafkaTemplate.send(JOB_EVENTS_TOPIC, event.eventId(), event).get(10, TimeUnit.SECONDS);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals("SUCCEEDED", jobRepository.findById(submission.getId())
                        .orElseThrow().getStatus().name()));

        jdbcTemplate.update("""
                UPDATE outbox_events
                SET lease_expires_at = DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 SECOND)
                WHERE event_id = ?
                """, pending.getEventId());
        assertEquals(1, dispatcher.dispatchAvailable());

        assertEquals(2, readBrokerRecordsForEvent(event.eventId()));
        OutboxEvent delivered = outboxRepository.findById(pending.getEventId()).orElseThrow();
        JobEntity completed = jobRepository.findById(submission.getId()).orElseThrow();
        assertEquals(OutboxEvent.Status.DELIVERED, delivered.getStatus());
        assertEquals(2, delivered.getAttemptCount());
        assertEquals(1, completed.getAttemptCount());
    }

    @Test
    void independentApplicationContextsShareMySqlAndKafkaWhileCompetingForOneJobClaim() throws Exception {
        String tenant = "multi-instance-" + UUID.randomUUID();
        quotaRepository.saveAndFlush(new TenantQuota(tenant, 5, 20));
        JobEntity job = new JobEntity(tenant, "echo", "{}", JobPriority.NORMAL,
                1, 3, UUID.randomUUID().toString());
        job.transitionTo(JobStatus.QUEUED);
        job.setScheduledAt(Instant.now());
        job = jobRepository.saveAndFlush(job);
        JobEvent event = new JobEvent(UUID.randomUUID().toString(), JobEvent.TYPE,
                JobEvent.CURRENT_SCHEMA_VERSION, job.getId(), tenant, job.getCorrelationId(), Instant.now());

        try (ConfigurableApplicationContext secondInstance = startSecondInstance()) {
            JobExecutionService secondExecutionService = secondInstance.getBean(JobExecutionService.class);
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService workers = Executors.newFixedThreadPool(2);
            try {
                Future<?> first = workers.submit(() -> {
                    start.await();
                    executionService.consume(event);
                    return null;
                });
                Future<?> second = workers.submit(() -> {
                    start.await();
                    secondExecutionService.consume(event);
                    return null;
                });
                start.countDown();
                first.get(30, TimeUnit.SECONDS);
                second.get(30, TimeUnit.SECONDS);
            } finally {
                workers.shutdownNow();
            }
        }

        JobEntity completed = jobRepository.findById(job.getId()).orElseThrow();
        assertEquals(JobStatus.SUCCEEDED, completed.getStatus());
        assertEquals(1, completed.getAttemptCount());
    }

    private ConfigurableApplicationContext startSecondInstance() {
        Map<String, Object> instanceProperties = new HashMap<>();
        instanceProperties.put("spring.datasource.url", MYSQL.getJdbcUrl());
        instanceProperties.put("spring.datasource.username", MYSQL.getUsername());
        instanceProperties.put("spring.datasource.password", MYSQL.getPassword());
        instanceProperties.put("spring.datasource.driver-class-name", MYSQL.getDriverClassName());
        instanceProperties.put("spring.flyway.enabled", "false");
        instanceProperties.put("spring.jpa.hibernate.ddl-auto", "none");
        instanceProperties.put("spring.kafka.bootstrap-servers", KAFKA.getBootstrapServers());
        instanceProperties.put("spring.kafka.consumer.group-id", "devflow-outbox-it-workers");
        instanceProperties.put("devflow.kafka.job-events-topic", JOB_EVENTS_TOPIC);
        instanceProperties.put("devflow.outbox.poll-interval-ms", "3600000");
        instanceProperties.put("devflow.worker.lease-heartbeat-ms", "3600000");
        instanceProperties.put("devflow.worker.recovery-interval-ms", "3600000");
        instanceProperties.put("spring.main.web-application-type", "none");
        return new SpringApplicationBuilder(DevFlowAiApplication.class)
                .profiles("production", "kafka")
                .web(WebApplicationType.NONE)
                .initializers(context -> context.getEnvironment().getPropertySources()
                        .addFirst(new MapPropertySource("second-instance-it", instanceProperties)))
                .run();
    }

    private com.devflowai.api.dto.JobResponse submitJob(String tenantPrefix) {
        String tenant = tenantPrefix + "-" + UUID.randomUUID();
        JobSubmissionRequest request = new JobSubmissionRequest("echo", "{}", JobPriority.NORMAL,
                1, 3, UUID.randomUUID().toString());
        return jobService.submitJob(tenant, request);
    }

    private OutboxEvent outboxFor(Long jobId) {
        return outboxRepository.findAll().stream()
                .filter(event -> event.getJobId().equals(jobId))
                .findFirst()
                .orElseThrow();
    }

    private int readBrokerRecordsForEvent(String eventId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "outbox-verification-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        int matches = 0;
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(JOB_EVENTS_TOPIC));
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
}
