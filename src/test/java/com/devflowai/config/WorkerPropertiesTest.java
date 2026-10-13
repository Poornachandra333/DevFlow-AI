package com.devflowai.config;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerPropertiesTest {
    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    void defaultsWorkerCapacityAndConcurrencyIndependently() {
        WorkerProperties properties = new WorkerProperties();

        assertEquals(25, properties.getClaimBatchSize());
        assertEquals(1, properties.getConcurrency());
        assertTrue(validator.validate(properties).isEmpty());
    }

    @Test
    void rejectsNonPositiveClaimCapacityAndConcurrency() {
        WorkerProperties properties = new WorkerProperties();
        properties.setClaimBatchSize(0);
        properties.setConcurrency(-1);

        assertEquals(2, validator.validate(properties).size());
    }
}
