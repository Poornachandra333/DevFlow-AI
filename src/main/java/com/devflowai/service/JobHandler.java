package com.devflowai.service;

public interface JobHandler {
    boolean supports(String jobType);

    String execute(JobExecutionContext context) throws Exception;
}
