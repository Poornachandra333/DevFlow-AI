package com.devflowai.api;

import com.devflowai.service.JobNotFoundException;
import com.devflowai.service.JobStateTransitionException;
import com.devflowai.service.JobSubmissionException;
import com.devflowai.ratelimit.RateLimitExceededException;
import com.devflowai.ratelimit.RateLimitStoreUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@RestControllerAdvice
public class JobApiExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(JobApiExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(MethodArgumentNotValidException ex) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", Instant.now());
        body.put("status", HttpStatus.BAD_REQUEST.value());
        body.put("error", "Validation failed");
        Map<String, String> errors = new LinkedHashMap<>();
        for (FieldError fieldError : ex.getBindingResult().getFieldErrors()) {
            errors.put(fieldError.getField(), fieldError.getDefaultMessage());
        }
        body.put("details", errors);
        return ResponseEntity.badRequest().body(body);
    }

    @ExceptionHandler(JobSubmissionException.class)
    public ResponseEntity<Map<String, Object>> handleSubmission(JobSubmissionException ex) {
        return build(HttpStatus.CONFLICT, "Job submission rejected", ex.getMessage());
    }

    @ExceptionHandler(JobNotFoundException.class)
    public ResponseEntity<Map<String, Object>> handleNotFound(JobNotFoundException ex) {
        return build(HttpStatus.NOT_FOUND, "Job not found", ex.getMessage());
    }

    @ExceptionHandler(JobStateTransitionException.class)
    public ResponseEntity<Map<String, Object>> handleTransition(JobStateTransitionException ex) {
        return build(HttpStatus.CONFLICT, "Invalid state transition", ex.getMessage());
    }

    @ExceptionHandler(RateLimitExceededException.class)
    public ResponseEntity<Map<String, Object>> handleRateLimit(RateLimitExceededException ex) {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header("Retry-After", Long.toString(ex.getRetryAfterSeconds()))
                .body(body(HttpStatus.TOO_MANY_REQUESTS, "Rate limit exceeded", ex.getMessage()));
    }

    @ExceptionHandler(RateLimitStoreUnavailableException.class)
    public ResponseEntity<Map<String, Object>> handleRateLimitStoreUnavailable(
            RateLimitStoreUnavailableException ex) {
        log.error("Redis rate-limit store is unavailable; refusing job submission", ex);
        return build(HttpStatus.SERVICE_UNAVAILABLE, "Rate limit service unavailable",
                "Job submission is temporarily unavailable");
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArgument(IllegalArgumentException ex) {
        return build(HttpStatus.BAD_REQUEST, "Invalid request", ex.getMessage());
    }

    private ResponseEntity<Map<String, Object>> build(HttpStatus status, String error, String message) {
        return ResponseEntity.status(status).body(body(status, error, message));
    }

    private Map<String, Object> body(HttpStatus status, String error, String message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", Instant.now());
        body.put("status", status.value());
        body.put("error", error);
        body.put("message", message);
        return body;
    }
}
