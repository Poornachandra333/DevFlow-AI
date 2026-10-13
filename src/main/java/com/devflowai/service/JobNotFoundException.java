package com.devflowai.service;

public class JobNotFoundException extends RuntimeException {
    public JobNotFoundException(Long jobId) {
        super("Job " + jobId + " was not found");
    }
}
