package com.devflowai.ratelimit;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;

@Service
@Profile("rate-limit")
public class TenantSubmissionRateLimiter {
    private static final String KEY_PREFIX = "devflow:rate-limit:job-submission:";

    private final RateLimitStore store;
    private final MeterRegistry metrics;
    private final int maxSubmissions;
    private final Duration window;

    public TenantSubmissionRateLimiter(
            RateLimitStore store,
            MeterRegistry metrics,
            @Value("${devflow.rate-limit.submissions-per-window:60}") int maxSubmissions,
            @Value("${devflow.rate-limit.window-seconds:60}") long windowSeconds) {
        if (maxSubmissions < 1 || windowSeconds < 1) {
            throw new IllegalArgumentException("Rate limit and window must be positive");
        }
        this.store = store;
        this.metrics = metrics;
        this.maxSubmissions = maxSubmissions;
        this.window = Duration.ofSeconds(windowSeconds);
    }

    public Decision acquire(String tenantId) {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("Tenant identity is required for rate limiting");
        }

        RateLimitStore.CounterSnapshot snapshot;
        try {
            snapshot = store.incrementAndExpire(keyFor(tenantId), window);
        } catch (DataAccessException ex) {
            metrics.counter("devflow.rate_limit.store_unavailable").increment();
            throw new RateLimitStoreUnavailableException(ex);
        }

        if (snapshot.count() <= maxSubmissions) {
            metrics.counter("devflow.rate_limit.allowed").increment();
            return new Decision(true, 0);
        }

        metrics.counter("devflow.rate_limit.rejected").increment();
        long retryAfterSeconds = Math.max(1, (snapshot.remainingMillis() + 999) / 1000);
        return new Decision(false, retryAfterSeconds);
    }

    private String keyFor(String tenantId) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(tenantId.getBytes(StandardCharsets.UTF_8));
            return KEY_PREFIX + HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is not available", ex);
        }
    }

    public record Decision(boolean allowed, long retryAfterSeconds) {
    }
}
