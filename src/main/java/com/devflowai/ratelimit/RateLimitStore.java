package com.devflowai.ratelimit;

import java.time.Duration;

public interface RateLimitStore {
    CounterSnapshot incrementAndExpire(String key, Duration window);

    record CounterSnapshot(long count, long remainingMillis) {
    }
}
