package com.devflowai.ratelimit;

public class RateLimitStoreUnavailableException extends RuntimeException {
    public RateLimitStoreUnavailableException(Throwable cause) {
        super("Rate limit service is temporarily unavailable", cause);
    }
}
