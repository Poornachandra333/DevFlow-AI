package com.devflowai.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
@Profile("kafka")
@EnableKafka
@EnableScheduling
public class KafkaWorkerConfig {

    @Bean
    public CommonErrorHandler kafkaConsumerErrorHandler(
            KafkaTemplate<Object, Object> kafkaTemplate,
            @Value("${devflow.kafka.job-events-topic:devflow.job-events.v1}") String topic,
            @Value("${devflow.kafka.retry.initial-interval-ms:1000}") long initialIntervalMs,
            @Value("${devflow.kafka.retry.max-elapsed-time-ms:30000}") long maxElapsedTimeMs) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate, (ConsumerRecord<?, ?> record, Exception exception) ->
                new org.apache.kafka.common.TopicPartition(topic + ".DLT", record.partition()));
        ExponentialBackOff backOff = new ExponentialBackOff(initialIntervalMs, 2.0);
        backOff.setMaxElapsedTime(maxElapsedTimeMs);
        return new DefaultErrorHandler(recoverer, backOff);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            CommonErrorHandler kafkaConsumerErrorHandler,
            WorkerProperties workerProperties) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(kafkaConsumerErrorHandler);
        factory.setConcurrency(workerProperties.getConcurrency());
        return factory;
    }
}
