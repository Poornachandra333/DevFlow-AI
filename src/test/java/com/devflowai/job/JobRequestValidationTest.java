package com.devflowai.job;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobRequestValidationTest {

    @Test
    void validRequestHasNoViolations() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();

        JobRequest request = new JobRequest(
                "tenant-a",
                "summarize-report",
                "{\"reportId\": 42}",
                JobPriority.HIGH,
                3,
                5,
                "job-tenant-a-42"
        );

        Set<ConstraintViolation<JobRequest>> violations = validator.validate(request);
        assertTrue(violations.isEmpty(), "valid request should not fail validation");
    }

    @Test
    void invalidRequestReportsMissingRequiredFields() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();

        JobRequest request = new JobRequest();
        request.setTenantId(" ");
        request.setJobName(" ");
        request.setPayload(" ");
        request.setPriority(JobPriority.NORMAL);
        request.setResourceUnits(1);
        request.setMaxAttempts(2);
        request.setIdempotencyKey(" ");

        Set<ConstraintViolation<JobRequest>> violations = validator.validate(request);
        assertEquals(4, violations.size());
    }
}
