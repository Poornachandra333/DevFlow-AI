package com.devflowai.service;

import org.springframework.stereotype.Component;

@Component
public class PayloadEchoJobHandler implements JobHandler {
    @Override
    public boolean supports(String jobType) {
        return true;
    }

    @Override
    public String execute(JobExecutionContext context) {
        return context.payload();
    }
}
