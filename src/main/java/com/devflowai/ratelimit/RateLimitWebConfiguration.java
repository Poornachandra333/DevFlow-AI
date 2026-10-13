package com.devflowai.ratelimit;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@Profile("rate-limit")
public class RateLimitWebConfiguration implements WebMvcConfigurer {
    private final JobSubmissionRateLimitInterceptor interceptor;

    public RateLimitWebConfiguration(JobSubmissionRateLimitInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(interceptor).addPathPatterns("/api/v1/jobs");
    }
}
