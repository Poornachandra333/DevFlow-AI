package com.devflowai.ratelimit;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;

import java.net.ServerSocket;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertThrows;

class RedisRateLimitStoreUnavailableTest {
    @Test
    void connectionRefusalFailsClosedWithoutContactingExternalRedis() throws Exception {
        int unusedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            unusedPort = socket.getLocalPort();
        }

        LettuceClientConfiguration clientConfiguration = LettuceClientConfiguration.builder()
                .commandTimeout(Duration.ofMillis(300))
                .build();
        LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration("127.0.0.1", unusedPort), clientConfiguration);
        try {
            connectionFactory.afterPropertiesSet();
            StringRedisTemplate template = new StringRedisTemplate(connectionFactory);
            template.afterPropertiesSet();
            TenantSubmissionRateLimiter limiter = new TenantSubmissionRateLimiter(
                    new RedisRateLimitStore(template), new SimpleMeterRegistry(), 2, 60);

            assertThrows(RateLimitStoreUnavailableException.class, () -> limiter.acquire("isolated-test-tenant"));
        } finally {
            connectionFactory.destroy();
        }
    }
}
