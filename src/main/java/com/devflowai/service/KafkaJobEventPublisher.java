package com.devflowai.service;

import com.devflowai.event.JobEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class KafkaJobEventPublisher implements JobEventPublisher {
    private final KafkaTemplate<String, JobEvent> kafkaTemplate;
    private final String topic;
    private final Duration acknowledgementTimeout;

    public KafkaJobEventPublisher(KafkaTemplate<String, JobEvent> kafkaTemplate,
                                  @Value("${devflow.kafka.job-events-topic:devflow.job-events.v1}") String topic,
                                  @Value("${devflow.kafka.publish-timeout-seconds:10}") long timeoutSeconds) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.acknowledgementTimeout = Duration.ofSeconds(timeoutSeconds);
    }

    @Override
    public void publish(JobEvent event) {
        try {
            kafkaTemplate.send(topic, event.eventId(), event)
                    .get(acknowledgementTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing job event " + event.eventId(), ex);
        } catch (ExecutionException | TimeoutException ex) {
            throw new IllegalStateException("Kafka did not acknowledge job event " + event.eventId(), ex);
        }
    }
}
