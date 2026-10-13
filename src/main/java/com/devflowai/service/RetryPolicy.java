package com.devflowai.service;

import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

@Component
public class RetryPolicy {
    private static final long MAX_DELAY_SECONDS = 300;
    private final DoubleSupplier jitter;

    public RetryPolicy() {
        this(() -> ThreadLocalRandom.current().nextDouble(0.5, 1.5));
    }

    RetryPolicy(DoubleSupplier jitter) {
        this.jitter = jitter;
    }

    public Duration delayForAttempt(int attempt) {
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be positive");
        }
        long exponential = 1L << Math.min(attempt - 1, 20);
        double factor = jitter.getAsDouble();
        if (factor < 0.5 || factor >= 1.5) {
            throw new IllegalStateException("jitter must be in [0.5, 1.5)");
        }
        long seconds = Math.min(MAX_DELAY_SECONDS, Math.max(1L, (long) (exponential * factor)));
        return Duration.ofSeconds(seconds);
    }
}
