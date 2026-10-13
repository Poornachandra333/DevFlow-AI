package com.devflowai.ratelimit;

import com.devflowai.api.JobApiExceptionHandler;
import com.devflowai.security.TenantContext;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers(disabledWithoutDocker = true)
class RedisRateLimitStoreIntegrationTest {
    @Container
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    private static LettuceConnectionFactory connectionFactory;
    private static StringRedisTemplate redisTemplate;
    private static RedisRateLimitStore store;

    @BeforeAll
    static void startRedis() {
        connectionFactory = new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
        connectionFactory.afterPropertiesSet();
        redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.afterPropertiesSet();
        store = new RedisRateLimitStore(redisTemplate);
    }

    @AfterAll
    static void stopRedis() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
    }

    @Test
    void atomicallyIncrementsAndSetsExpiry() {
        String key = "test:rate-limit:expiry:" + UUID.randomUUID();

        RateLimitStore.CounterSnapshot first = store.incrementAndExpire(key, Duration.ofSeconds(30));
        RateLimitStore.CounterSnapshot second = store.incrementAndExpire(key, Duration.ofSeconds(30));

        assertEquals(1, first.count());
        assertEquals(2, second.count());
        assertTrue(second.remainingMillis() > 0 && second.remainingMillis() <= 30_000);
    }

    @Test
    void concurrentIncrementsAreNotLost() throws Exception {
        String key = "test:rate-limit:concurrent:" + UUID.randomUUID();
        int requestCount = 40;
        try (var executor = Executors.newFixedThreadPool(8)) {
            List<java.util.concurrent.Future<RateLimitStore.CounterSnapshot>> results = new ArrayList<>();
            for (int i = 0; i < requestCount; i++) {
                results.add(executor.submit(() -> store.incrementAndExpire(key, Duration.ofMinutes(1))));
            }

            long greatestCount = 0;
            for (var result : results) {
                greatestCount = Math.max(greatestCount, result.get().count());
            }
            assertEquals(requestCount, greatestCount);
        }
    }

    @Test
    void fixedWindowExpiresAndCounterStartsOver() throws Exception {
        String key = "test:rate-limit:expiry-reset:" + UUID.randomUUID();

        assertEquals(1, store.incrementAndExpire(key, Duration.ofMillis(250)).count());
        assertEquals(2, store.incrementAndExpire(key, Duration.ofMillis(250)).count());

        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (Boolean.TRUE.equals(redisTemplate.hasKey(key)) && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }

        assertFalse(Boolean.TRUE.equals(redisTemplate.hasKey(key)));
        assertEquals(1, store.incrementAndExpire(key, Duration.ofMillis(250)).count());
    }

    @Test
    void configuredLimitIsTenantScopedAndProducesRetryAfterResponse() {
        TenantSubmissionRateLimiter limiter = new TenantSubmissionRateLimiter(
                store, new SimpleMeterRegistry(), 1, 60);
        JobSubmissionRateLimitInterceptor interceptor = new JobSubmissionRateLimitInterceptor(limiter);
        JobApiExceptionHandler exceptionHandler = new JobApiExceptionHandler();
        String tenantA = "integration-tenant-a-" + UUID.randomUUID();
        String tenantB = "integration-tenant-b-" + UUID.randomUUID();

        try {
            TenantContext.setTenantId(tenantA);
            assertTrue(interceptor.preHandle(new MockHttpServletRequest("POST", "/api/v1/jobs"),
                    new MockHttpServletResponse(), new Object()));
            RateLimitExceededException exceeded = assertThrows(RateLimitExceededException.class,
                    () -> interceptor.preHandle(new MockHttpServletRequest("POST", "/api/v1/jobs"),
                            new MockHttpServletResponse(), new Object()));
            var response = exceptionHandler.handleRateLimit(exceeded);

            assertEquals(HttpStatus.TOO_MANY_REQUESTS, response.getStatusCode());
            assertTrue(Long.parseLong(response.getHeaders().getFirst("Retry-After")) > 0);

            TenantContext.setTenantId(tenantB);
            assertTrue(interceptor.preHandle(new MockHttpServletRequest("POST", "/api/v1/jobs"),
                    new MockHttpServletResponse(), new Object()));
        } finally {
            TenantContext.clear();
        }
    }
}
