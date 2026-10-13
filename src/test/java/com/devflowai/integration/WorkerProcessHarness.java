package com.devflowai.integration;

import com.devflowai.DevFlowAiApplication;
import com.devflowai.event.JobEvent;
import com.devflowai.service.JobEventPublisher;
import com.devflowai.service.JobExecutionContext;
import com.devflowai.service.JobExecutionService;
import com.devflowai.service.JobHandler;
import com.devflowai.service.KafkaJobEventPublisher;
import com.devflowai.service.OutboxDispatchService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.MapPropertySource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public final class WorkerProcessHarness {
    private WorkerProcessHarness() {
    }

    public static void main(String[] args) {
        if (args.length != 10) {
            throw new IllegalArgumentException("Expected mode, database, Kafka, job, and marker arguments");
        }
        Map<String, Object> properties = new HashMap<>();
        properties.put("spring.datasource.url", args[1]);
        properties.put("spring.datasource.username", args[2]);
        properties.put("spring.datasource.password", args[3]);
        properties.put("spring.datasource.driver-class-name", "com.mysql.cj.jdbc.Driver");
        properties.put("spring.flyway.enabled", "false");
        properties.put("spring.jpa.hibernate.ddl-auto", "none");
        properties.put("spring.kafka.bootstrap-servers", args[4]);
        properties.put("spring.kafka.consumer.group-id", "devflow-process-harness");
        properties.put("devflow.kafka.job-events-topic", args[5]);
        properties.put("devflow.worker.lease-duration-seconds", "2");
        properties.put("devflow.worker.lease-heartbeat-ms", "3600000");
        properties.put("devflow.worker.recovery-interval-ms", "3600000");
        properties.put("devflow.outbox.poll-interval-ms", "3600000");
        properties.put("devflow.test.process-mode", args[0]);
        properties.put("devflow.test.job-id", args[6]);
        properties.put("devflow.test.tenant-id", args[7]);
        properties.put("devflow.test.correlation-id", args[8]);
        properties.put("devflow.test.marker", args[9]);
        properties.put("spring.main.web-application-type", "none");
        try (var applicationContext = new SpringApplicationBuilder(DevFlowAiApplication.class, HarnessConfiguration.class)
                .profiles("production", "kafka")
                .web(WebApplicationType.NONE)
                .initializers(context -> context.getEnvironment().getPropertySources()
                        .addFirst(new MapPropertySource("process-harness", properties)))
                .run()) {
            if ("worker".equals(args[0])) {
                applicationContext.getBean(JobExecutionService.class).consume(new JobEvent("process-crash-event",
                        JobEvent.TYPE, JobEvent.CURRENT_SCHEMA_VERSION, Long.parseLong(args[6]),
                        args[7], args[8], Instant.now()));
            } else if ("outbox".equals(args[0])) {
                applicationContext.getBean(OutboxDispatchService.class).dispatchAvailable();
            } else {
                throw new IllegalArgumentException("Unsupported process mode: " + args[0]);
            }
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    public static class HarnessConfiguration {
        @Bean
        @org.springframework.core.annotation.Order(org.springframework.core.Ordered.HIGHEST_PRECEDENCE)
        JobHandler processBlockingHandler(@Value("${devflow.test.marker}") String marker) {
            return new JobHandler() {
                @Override
                public boolean supports(String jobType) {
                    return "process-crash-it".equals(jobType);
                }

                @Override
                public String execute(JobExecutionContext context) throws Exception {
                    signalAndBlock(Path.of(marker));
                    return "must-not-be-persisted";
                }
            };
        }

        @Bean
        @Primary
        JobEventPublisher acknowledgementBlockingPublisher(KafkaJobEventPublisher delegate,
                                                            @Value("${devflow.test.marker}") String marker) {
            return event -> {
                delegate.publish(event);
                try {
                    signalAndBlock(Path.of(marker));
                } catch (Exception exception) {
                    throw new IllegalStateException("Failed at the outbox acknowledgement test barrier", exception);
                }
            };
        }

        private static void signalAndBlock(Path marker) throws Exception {
            Files.writeString(marker, "reached");
            while (true) {
                Thread.sleep(1_000);
            }
        }
    }
}
