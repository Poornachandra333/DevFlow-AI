package com.devflowai.ratelimit;

import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

@Component
@Profile("rate-limit")
public class RedisRateLimitStore implements RateLimitStore {
    private static final RedisScript<String> INCREMENT_AND_EXPIRE = new DefaultRedisScript<>("""
            local count = redis.call('INCR', KEYS[1])
            if count == 1 then
                redis.call('PEXPIRE', KEYS[1], ARGV[1])
            end
            return tostring(count) .. ':' .. tostring(redis.call('PTTL', KEYS[1]))
            """, String.class);

    private final StringRedisTemplate redisTemplate;

    public RedisRateLimitStore(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public CounterSnapshot incrementAndExpire(String key, Duration window) {
        String result = redisTemplate.execute(INCREMENT_AND_EXPIRE, List.of(key),
                Long.toString(window.toMillis()));
        if (result == null) {
            throw new IllegalStateException("Redis rate limit script returned no result");
        }

        String[] values = result.split(":", -1);
        if (values.length != 2) {
            throw new IllegalStateException("Redis rate limit script returned an invalid result");
        }
        try {
            return new CounterSnapshot(Long.parseLong(values[0]), Long.parseLong(values[1]));
        } catch (NumberFormatException ex) {
            throw new IllegalStateException("Redis rate limit script returned an invalid result", ex);
        }
    }
}
