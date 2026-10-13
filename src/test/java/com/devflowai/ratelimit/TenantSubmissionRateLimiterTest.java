package com.devflowai.ratelimit;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TenantSubmissionRateLimiterTest {
    private final RateLimitStore store = mock(RateLimitStore.class);
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    private final TenantSubmissionRateLimiter limiter =
            new TenantSubmissionRateLimiter(store, metrics, 2, 60);

    @Test
    void permitsRequestsUpToConfiguredLimitAndReturnsWindowRemainder() {
        when(store.incrementAndExpire(anyString(), eq(Duration.ofSeconds(60))))
                .thenReturn(new RateLimitStore.CounterSnapshot(1, 60_000))
                .thenReturn(new RateLimitStore.CounterSnapshot(2, 42_100))
                .thenReturn(new RateLimitStore.CounterSnapshot(3, 42_100));

        assertTrue(limiter.acquire("tenant-a").allowed());
        assertTrue(limiter.acquire("tenant-a").allowed());
        TenantSubmissionRateLimiter.Decision rejected = limiter.acquire("tenant-a");

        assertFalse(rejected.allowed());
        assertEquals(43, rejected.retryAfterSeconds());
        assertEquals(1.0, metrics.counter("devflow.rate_limit.rejected").count());
    }

    @Test
    void redisStoreFailureFailsClosedAndIsMeasured() {
        when(store.incrementAndExpire(anyString(), eq(Duration.ofSeconds(60))))
                .thenThrow(new DataAccessResourceFailureException("Redis unavailable"));

        assertThrows(RateLimitStoreUnavailableException.class, () -> limiter.acquire("tenant-a"));

        assertEquals(1.0, metrics.counter("devflow.rate_limit.store_unavailable").count());
    }

    @Test
    void tenantsUseHashedKeysAndDoNotShareCounters() {
        when(store.incrementAndExpire(anyString(), eq(Duration.ofSeconds(60))))
                .thenReturn(new RateLimitStore.CounterSnapshot(1, 60_000));

        limiter.acquire("tenant-a");
        limiter.acquire("tenant-b");

        var keys = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(store, org.mockito.Mockito.times(2))
                .incrementAndExpire(keys.capture(), eq(Duration.ofSeconds(60)));
        assertFalse(keys.getAllValues().getFirst().contains("tenant-a"));
        assertFalse(keys.getAllValues().getLast().contains("tenant-b"));
        assertTrue(!keys.getAllValues().getFirst().equals(keys.getAllValues().getLast()));
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThrows(IllegalArgumentException.class,
                () -> new TenantSubmissionRateLimiter(store, metrics, 0, 60));
        assertThrows(IllegalArgumentException.class,
                () -> new TenantSubmissionRateLimiter(store, metrics, 10, 0));
    }
}
