package com.devflowai.ratelimit;

import com.devflowai.api.JobApiExceptionHandler;
import com.devflowai.security.TenantContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JobSubmissionRateLimitInterceptorTest {
    private final TenantSubmissionRateLimiter rateLimiter = mock(TenantSubmissionRateLimiter.class);
    private final JobSubmissionRateLimitInterceptor interceptor =
            new JobSubmissionRateLimitInterceptor(rateLimiter);

    @AfterEach
    void clearTenantContext() {
        TenantContext.clear();
    }

    @Test
    void rejectsLimitedSubmissionWithRetryAfterHeader() {
        TenantContext.setTenantId("tenant-a");
        when(rateLimiter.acquire("tenant-a"))
                .thenReturn(new TenantSubmissionRateLimiter.Decision(false, 12));
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/v1/jobs");
        MockHttpServletResponse response = new MockHttpServletResponse();

        RateLimitExceededException exception = assertThrows(RateLimitExceededException.class,
                () -> interceptor.preHandle(request, response, new Object()));
        var apiResponse = new JobApiExceptionHandler().handleRateLimit(exception);

        assertEquals(429, apiResponse.getStatusCode().value());
        assertEquals("12", apiResponse.getHeaders().getFirst("Retry-After"));
        assertEquals(429, apiResponse.getBody().get("status"));
    }

    @Test
    void nonPostRequestsDoNotConsumeSubmissionQuota() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/v1/jobs");

        assertTrue(interceptor.preHandle(request, new MockHttpServletResponse(), new Object()));
        verify(rateLimiter, never()).acquire(org.mockito.ArgumentMatchers.anyString());
    }

    @Test
    void mapsRedisFailureToServiceUnavailable() {
        var apiResponse = new JobApiExceptionHandler().handleRateLimitStoreUnavailable(
                new RateLimitStoreUnavailableException(new DataAccessResourceFailureException("Redis unavailable")));

        assertEquals(503, apiResponse.getStatusCode().value());
        assertEquals("Rate limit service unavailable", apiResponse.getBody().get("error"));
    }
}
