package com.devflowai.service;

public class JobStateTransitionException extends RuntimeException {
    public JobStateTransitionException(String message) {
        super(message);
    }
}
