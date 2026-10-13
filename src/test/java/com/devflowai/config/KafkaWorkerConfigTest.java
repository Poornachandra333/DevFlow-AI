package com.devflowai.config;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

class KafkaWorkerConfigTest {
    @Test
    void appliesWorkerConcurrencyToKafkaListenerContainer() {
        WorkerProperties properties = new WorkerProperties();
        properties.setConcurrency(3);
        KafkaWorkerConfig config = new KafkaWorkerConfig();
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                config.kafkaListenerContainerFactory(
                        mock(ConsumerFactory.class), mock(org.springframework.kafka.listener.CommonErrorHandler.class),
                        properties);

        MessageListenerContainer container = factory.createContainer("devflow-test-topic");

        assertEquals(3, ((org.springframework.kafka.listener.ConcurrentMessageListenerContainer<?, ?>) container)
                .getConcurrency());
        container.destroy();
    }
}
