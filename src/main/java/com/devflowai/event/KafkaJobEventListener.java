package com.devflowai.event;

import com.devflowai.service.JobExecutionService;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Profile("kafka")
public class KafkaJobEventListener {
    private final JobExecutionService executionService;

    public KafkaJobEventListener(JobExecutionService executionService) {
        this.executionService = executionService;
    }

    @KafkaListener(topics = "${devflow.kafka.job-events-topic:devflow.job-events.v1}")
    public void onJobEvent(JobEvent event) {
        executionService.consume(event);
    }
}
