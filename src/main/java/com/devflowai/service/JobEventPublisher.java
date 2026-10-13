package com.devflowai.service;

import com.devflowai.event.JobEvent;

public interface JobEventPublisher {
    void publish(JobEvent event);
}
