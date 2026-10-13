package com.devflowai.ratelimit;

import com.devflowai.security.TenantContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

@Component
@Profile("rate-limit")
public class JobSubmissionRateLimitInterceptor implements HandlerInterceptor {
    private final TenantSubmissionRateLimiter rateLimiter;

    public JobSubmissionRateLimitInterceptor(TenantSubmissionRateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (!"POST".equals(request.getMethod())) {
            return true;
        }

        TenantSubmissionRateLimiter.Decision decision = rateLimiter.acquire(TenantContext.getTenantId());
        if (!decision.allowed()) {
            throw new RateLimitExceededException(decision.retryAfterSeconds());
        }
        return true;
    }
}
