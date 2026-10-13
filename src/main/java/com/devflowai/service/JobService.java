package com.devflowai.service;

import com.devflowai.api.dto.JobResponse;
import com.devflowai.api.dto.JobSubmissionRequest;
import com.devflowai.api.dto.JobSummaryResponse;
import com.devflowai.domain.JobEntity;
import com.devflowai.domain.JobExecutionHistory;
import com.devflowai.domain.OutboxEvent;
import com.devflowai.domain.TenantQuota;
import com.devflowai.event.JobEvent;
import com.devflowai.job.JobPriority;
import com.devflowai.job.JobStatus;
import com.devflowai.repository.JobExecutionHistoryRepository;
import com.devflowai.repository.JobRepository;
import com.devflowai.repository.OutboxEventRepository;
import com.devflowai.repository.TenantQuotaRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class JobService {
    private static final List<JobStatus> ACTIVE_JOB_STATUSES = List.of(
            JobStatus.ACCEPTED, JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.RETRY_SCHEDULED);
    private static final int MAX_INTEGRITY_RETRIES = 3;

    private final JobRepository jobRepository;
    private final JobExecutionHistoryRepository historyRepository;
    private final TenantQuotaRepository tenantQuotaRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;

    public JobService(JobRepository jobRepository,
                      JobExecutionHistoryRepository historyRepository,
                      TenantQuotaRepository tenantQuotaRepository,
                      OutboxEventRepository outboxEventRepository,
                      ObjectMapper objectMapper,
                      PlatformTransactionManager transactionManager) {
        this.jobRepository = jobRepository;
        this.historyRepository = historyRepository;
        this.tenantQuotaRepository = tenantQuotaRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    public JobResponse submitJob(String tenantId, JobSubmissionRequest request) {
        String normalizedTenantId = normalizeTenantId(tenantId);
        String key = Optional.ofNullable(request.getIdempotencyKey()).map(String::trim).orElse("");
        if (key.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey is required");
        }

        for (int attempt = 0; attempt < MAX_INTEGRITY_RETRIES; attempt++) {
            try {
                JobResponse response = transactionTemplate.execute(status ->
                        submitJobInTransaction(normalizedTenantId, key, request));
                if (response == null) {
                    throw new IllegalStateException("Job submission transaction returned no response");
                }
                return response;
            } catch (DataIntegrityViolationException ex) {
                Optional<JobEntity> duplicate = jobRepository.findByTenantAndIdempotencyKey(normalizedTenantId, key);
                if (duplicate.isPresent()) {
                    return toResponse(duplicate.get());
                }
                if (attempt == MAX_INTEGRITY_RETRIES - 1) {
                    throw ex;
                }
            }
        }

        throw new IllegalStateException("Job submission exhausted integrity retries");
    }

    private JobResponse submitJobInTransaction(String tenantId, String idempotencyKey,
                                               JobSubmissionRequest request) {
        Optional<JobEntity> existing = jobRepository.findByTenantAndIdempotencyKey(tenantId, idempotencyKey);
        if (existing.isPresent()) {
            return toResponse(existing.get());
        }

        TenantQuota quota = tenantQuotaRepository.findByTenantIdForUpdate(tenantId)
                .orElseGet(() -> tenantQuotaRepository.saveAndFlush(new TenantQuota(tenantId, 5, 50)));

        long activeJobs = jobRepository.countByTenantIdAndStatusIn(tenantId, ACTIVE_JOB_STATUSES);
        if (activeJobs >= quota.getMaxConcurrentJobs()) {
            throw new JobSubmissionException("Tenant quota exceeded for active jobs");
        }

        long activeResourceUnits = jobRepository.sumResourceUnitsByTenantAndStatusIn(tenantId, ACTIVE_JOB_STATUSES);
        if (request.getResourceUnits() > quota.getMaxResourceUnits()
                || activeResourceUnits + request.getResourceUnits() > quota.getMaxResourceUnits()) {
            throw new JobSubmissionException("Job resource requirements exceed tenant quota");
        }

        JobEntity job = new JobEntity(tenantId, request.getJobType(), request.getPayload(),
                request.getPriority() == null ? JobPriority.NORMAL : request.getPriority(),
                request.getResourceUnits(), request.getMaxAttempts(), idempotencyKey);
        job.setScheduledAt(Instant.now());
        jobRepository.save(job);

        JobExecutionHistory history = new JobExecutionHistory(job, JobStatus.ACCEPTED, "Job accepted for scheduling");
        historyRepository.save(history);
        job.getExecutionHistory().add(history);

        job.transitionTo(JobStatus.QUEUED);
        jobRepository.save(job);

        JobExecutionHistory queued = new JobExecutionHistory(job, JobStatus.QUEUED, "Job queued for processing");
        historyRepository.save(queued);

        String eventId = UUID.randomUUID().toString();
        JobEvent event = new JobEvent(eventId, JobEvent.TYPE, JobEvent.CURRENT_SCHEMA_VERSION,
                job.getId(), tenantId, job.getCorrelationId(), Instant.now());
        try {
            outboxEventRepository.save(new OutboxEvent(event.eventId(), event.eventType(), event.schemaVersion(),
                    event.jobId(), event.correlationId(), objectMapper.writeValueAsString(event), Instant.now()));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Unable to serialize job outbox event", ex);
        }

        JobResponse response = toResponse(job);
        return response;
    }

    @Transactional(readOnly = true)
    public JobResponse getJobById(String tenantId, Long jobId) {
        JobEntity job = jobRepository.findOwnedJob(normalizeTenantId(tenantId), jobId)
                .orElseThrow(() -> new JobNotFoundException(jobId));
        return toResponse(job);
    }

    @Transactional(readOnly = true)
    public Page<JobSummaryResponse> listJobs(String tenantId, JobStatus status, JobPriority priority, Pageable pageable) {
        String normalizedTenantId = normalizeTenantId(tenantId);
        return jobRepository.findByTenantAndFilters(normalizedTenantId, status, priority, pageable)
                .map(this::toSummary);
    }

    @Transactional(readOnly = true)
    public List<JobResponse> getExecutionHistory(String tenantId, Long jobId) {
        String normalizedTenantId = normalizeTenantId(tenantId);
        JobEntity job = jobRepository.findOwnedJob(normalizedTenantId, jobId)
                .orElseThrow(() -> new JobNotFoundException(jobId));

        List<JobResponse> responses = new ArrayList<>();
        for (JobExecutionHistory history : historyRepository.findByJobIdOrderByCreatedAtAsc(job.getId())) {
            responses.add(new JobResponse(
                    history.getJob().getId(),
                    history.getJob().getTenantId(),
                    history.getJob().getJobType(),
                    history.getStatus(),
                    history.getJob().getPriority(),
                    history.getJob().getResourceUnits(),
                    history.getJob().getMaxAttempts(),
                    history.getJob().getAttemptCount(),
                    history.getJob().getPayload(),
                    history.getJob().getIdempotencyKey(),
                    history.getJob().getCreatedAt(),
                    history.getJob().getUpdatedAt(),
                    history.getJob().getStartedAt(),
                    history.getJob().getCompletedAt(),
                    history.getJob().getFailureReason()));
        }
        return responses;
    }

    @Transactional
    public JobResponse cancelJob(String tenantId, Long jobId) {
        JobEntity job = jobRepository.findOwnedJob(normalizeTenantId(tenantId), jobId)
                .orElseThrow(() -> new JobNotFoundException(jobId));

        if (job.getStatus().isTerminal()) {
            throw new JobStateTransitionException("Cannot cancel a terminal job");
        }
        if (job.getStatus() == JobStatus.CANCELLED) {
            throw new JobStateTransitionException("Job is already cancelled");
        }

        job.transitionTo(JobStatus.CANCELLED);
        job.setCompletedAt(Instant.now());
        historyRepository.save(new JobExecutionHistory(job, JobStatus.CANCELLED, "Job cancelled by request"));
        return toResponse(jobRepository.save(job));
    }

    @Transactional
    public JobResponse retryJob(String tenantId, Long jobId) {
        JobEntity job = jobRepository.findOwnedJob(normalizeTenantId(tenantId), jobId)
                .orElseThrow(() -> new JobNotFoundException(jobId));

        if (job.getStatus() != JobStatus.FAILED) {
            throw new JobStateTransitionException("Only failed jobs may be retried");
        }
        if (job.getAttemptCount() >= job.getMaxAttempts()) {
            throw new JobStateTransitionException("Job has reached its max retry count");
        }

        job.transitionTo(JobStatus.RETRY_SCHEDULED);
        job.incrementAttemptCount();
        job.setFailureReason(null);
        historyRepository.save(new JobExecutionHistory(job, JobStatus.RETRY_SCHEDULED, "Retry scheduled"));
        return toResponse(jobRepository.save(job));
    }

    @Transactional(readOnly = true)
    public JobStatus evaluateStatus(String tenantId, Long jobId) {
        return jobRepository.findOwnedJob(normalizeTenantId(tenantId), jobId)
                .map(JobEntity::getStatus)
                .orElseThrow(() -> new JobNotFoundException(jobId));
    }

    private String normalizeTenantId(String tenantId) {
        if (tenantId == null || tenantId.isBlank()) {
            return "dev-tenant";
        }
        return tenantId.trim();
    }

    private JobResponse toResponse(JobEntity job) {
        JobResponse response = new JobResponse(
                job.getId(),
                job.getTenantId(),
                job.getJobType(),
                job.getStatus(),
                job.getPriority(),
                job.getResourceUnits(),
                job.getMaxAttempts(),
                job.getAttemptCount(),
                job.getPayload(),
                job.getIdempotencyKey(),
                job.getCreatedAt(),
                job.getUpdatedAt(),
                job.getStartedAt(),
                job.getCompletedAt(),
                job.getFailureReason());
        response.setResult(job.getResult());
        response.setCorrelationId(job.getCorrelationId());
        return response;
    }

    private JobSummaryResponse toSummary(JobEntity job) {
        return new JobSummaryResponse(
                job.getId(),
                job.getTenantId(),
                job.getJobType(),
                job.getStatus(),
                job.getPriority(),
                job.getResourceUnits(),
                job.getCreatedAt(),
                job.getUpdatedAt());
    }

}
