package com.devflowai.integration;

import com.devflowai.domain.JobEntity;
import com.devflowai.domain.TenantQuota;
import com.devflowai.event.JobEvent;
import com.devflowai.job.JobPriority;
import com.devflowai.job.JobStatus;
import com.devflowai.repository.JobRepository;
import com.devflowai.repository.TenantQuotaRepository;
import com.devflowai.service.JobExecutionContext;
import com.devflowai.service.JobHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:devflow-kafka-it;MODE=MySQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.flyway.enabled=false",
        "devflow.kafka.retry.initial-interval-ms=50",
        "devflow.kafka.retry.max-elapsed-time-ms=250"
})
@ActiveProfiles({"production", "kafka"})
class KafkaWorkerIntegrationTest {
    private static final String JOB_EVENTS_TOPIC = "devflow.kafka-it.events";

    @Container
    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;
    @Autowired
    private JobRepository jobRepository;
    @Autowired
    private TenantQuotaRepository quotaRepository;
    @Autowired
    private KafkaIntegrationJobHandler handler;

    @DynamicPropertySource
    static void configureKafka(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("devflow.kafka.job-events-topic", () -> JOB_EVENTS_TOPIC);
        registry.add("devflow.worker.concurrency", () -> 1);
    }

    @Test
    void duplicateBrokerDeliveryDoesNotExecuteJobTwice() throws Exception {
        String tenantId = "kafka-duplicate-" + UUID.randomUUID();
        JobEntity firstJob = createQueuedJob(tenantId);
        JobEvent firstEvent = eventFor(firstJob);

        publish(firstEvent);
        Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(JobStatus.SUCCEEDED, jobRepository.findById(firstJob.getId()).orElseThrow().getStatus()));
        assertEquals(1, handler.invocations(firstJob.getId()));

        publish(firstEvent);
        JobEntity markerJob = createQueuedJob("kafka-marker-" + UUID.randomUUID());
        publish(eventFor(markerJob));

        Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(JobStatus.SUCCEEDED, jobRepository.findById(markerJob.getId()).orElseThrow().getStatus()));
        assertEquals(1, handler.invocations(firstJob.getId()));
        assertEquals(1, handler.invocations(markerJob.getId()));
    }

    @Test
    void invalidPersistedJobReferenceIsRetriedAndSentToDeadLetterTopic() throws Exception {
        String tenantId = "kafka-dlt-" + UUID.randomUUID();
        JobEvent invalidReference = new JobEvent(UUID.randomUUID().toString(), JobEvent.TYPE,
                JobEvent.CURRENT_SCHEMA_VERSION, Long.MAX_VALUE, tenantId, UUID.randomUUID().toString(),
                Instant.now());

        publish(invalidReference);

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "devflow-dlt-check-" + UUID.randomUUID());
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(java.util.List.of(JOB_EVENTS_TOPIC + ".DLT"));
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            boolean foundEvent = false;
            while (!foundEvent && System.nanoTime() < deadline) {
                var records = consumer.poll(Duration.ofMillis(250));
                for (var record : records) {
                    if (record.value() != null && record.value().contains(invalidReference.eventId())) {
                        foundEvent = true;
                        break;
                    }
                }
            }
            assertTrue(foundEvent, "The invalid event was not published to the Kafka DLT");
        }
    }

    private JobEntity createQueuedJob(String tenantId) {
        quotaRepository.saveAndFlush(new TenantQuota(tenantId, 5, 20));
        JobEntity job = new JobEntity(tenantId, "kafka-integration-task", "{}", JobPriority.NORMAL,
                1, 2, UUID.randomUUID().toString());
        job.transitionTo(JobStatus.QUEUED);
        job.setScheduledAt(Instant.now());
        return jobRepository.saveAndFlush(job);
    }

    private JobEvent eventFor(JobEntity job) {
        return new JobEvent(UUID.randomUUID().toString(), JobEvent.TYPE, JobEvent.CURRENT_SCHEMA_VERSION,
                job.getId(), job.getTenantId(), job.getCorrelationId(), Instant.now());
    }

    private void publish(JobEvent event) throws Exception {
        kafkaTemplate.send(JOB_EVENTS_TOPIC, "partition-key", event).get(10, TimeUnit.SECONDS);
    }

    @TestConfiguration
    static class IntegrationConfiguration {
        @Bean
        @Order(Ordered.HIGHEST_PRECEDENCE)
        KafkaIntegrationJobHandler kafkaIntegrationJobHandler() {
            return new KafkaIntegrationJobHandler();
        }
    }

    static class KafkaIntegrationJobHandler implements JobHandler {
        private final ConcurrentHashMap<Long, AtomicInteger> invocations = new ConcurrentHashMap<>();

        @Override
        public boolean supports(String jobType) {
            return "kafka-integration-task".equals(jobType);
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
