package com.devflowai.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RetryPolicyTest {

    @Test
    void exponentialDelayIsJitteredAndCapped() {
        RetryPolicy policy = new RetryPolicy(() -> 1.0);

        assertEquals(1, policy.delayForAttempt(1).toSeconds());
        assertEquals(2, policy.delayForAttempt(2).toSeconds());
        assertEquals(4, policy.delayForAttempt(3).toSeconds());
        assertEquals(300, policy.delayForAttempt(20).toSeconds());
    }

    @Test
    void rejectsInvalidAttemptNumbers() {
        RetryPolicy policy = new RetryPolicy(() -> 1.0);

        assertThrows(IllegalArgumentException.class, () -> policy.delayForAttempt(0));
    }
}
