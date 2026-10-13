package com.devflowai.service;

import com.devflowai.config.WorkerProperties;
import com.devflowai.domain.JobEntity;
import com.devflowai.domain.JobExecutionHistory;
import com.devflowai.domain.OutboxEvent;
import com.devflowai.domain.TenantQuota;
import com.devflowai.event.JobEvent;
import com.devflowai.job.JobRecord;
import com.devflowai.job.JobScheduler;
import com.devflowai.job.JobStatus;
import com.devflowai.repository.JobExecutionHistoryRepository;
import com.devflowai.repository.JobRepository;
import com.devflowai.repository.OutboxEventRepository;
import com.devflowai.repository.TenantQuotaRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.slf4j.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class JobExecutionService {
    private static final Logger log = LoggerFactory.getLogger(JobExecutionService.class);
    private static final List<JobStatus> CLAIMABLE = List.of(JobStatus.QUEUED);

    private final JobRepository jobRepository;
    private final JobExecutionHistoryRepository historyRepository;
    private final TenantQuotaRepository quotaRepository;
    private final OutboxEventRepository outboxRepository;
    private final JobScheduler scheduler;
    private final List<JobHandler> handlers;
    private final RetryPolicy retryPolicy;
    private final Validator validator;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactions;
    private final Clock clock;
    private final MeterRegistry metrics;
    private final Duration leaseDuration;
    private final WorkerProperties workerProperties;
    private final AtomicInteger activeExecutions = new AtomicInteger();
    private final DistributionSummary claimBatchSizeMetric;
    private final Map<Long, ActiveLease> activeLeases = new ConcurrentHashMap<>();

    public JobExecutionService(JobRepository jobRepository,
                               JobExecutionHistoryRepository historyRepository,
                               TenantQuotaRepository quotaRepository,
                               OutboxEventRepository outboxRepository,
                               JobScheduler scheduler,
                               List<JobHandler> handlers,
                               RetryPolicy retryPolicy,
                               Validator validator,
                               ObjectMapper objectMapper,
                               PlatformTransactionManager transactionManager,
                               Clock clock,
                               MeterRegistry metrics,
                               WorkerProperties workerProperties,
                               @Value("${devflow.worker.lease-duration-seconds:60}") long leaseSeconds) {
        this.jobRepository = jobRepository;
        this.historyRepository = historyRepository;
        this.quotaRepository = quotaRepository;
        this.outboxRepository = outboxRepository;
        this.scheduler = scheduler;
        this.handlers = List.copyOf(handlers);
        this.retryPolicy = retryPolicy;
        this.validator = validator;
        this.objectMapper = objectMapper;
        this.transactions = new TransactionTemplate(transactionManager);
        this.clock = clock;
        this.metrics = metrics;
        this.workerProperties = workerProperties;
        this.leaseDuration = Duration.ofSeconds(leaseSeconds);
        Gauge.builder("devflow.jobs.running", jobRepository,
                store -> store.countByStatus(JobStatus.RUNNING)).register(metrics);
        Gauge.builder("devflow.jobs.dead-lettered", jobRepository,
                store -> store.countByStatus(JobStatus.DEAD_LETTER)).register(metrics);
        Gauge.builder("devflow.worker.active", activeExecutions, AtomicInteger::get).register(metrics);
        this.claimBatchSizeMetric = DistributionSummary.builder("devflow.worker.claim.batch.size")
                .register(metrics);
    }

    public void consume(JobEvent event) {
        validateEvent(event);
        Boolean belongsToPersistedJob = transactions.execute(status -> jobRepository.findById(event.jobId())
                .map(job -> job.getTenantId().equals(event.tenantId())
                        && job.getCorrelationId().equals(event.correlationId()))
                .orElse(false));
        if (!Boolean.TRUE.equals(belongsToPersistedJob)) {
            throw new IllegalArgumentException("Job event does not match a persisted job");
        }
        long batchStarted = System.nanoTime();
        List<ClaimedJob> batch = claimBatch();
        claimBatchSizeMetric.record(batch.size());
        metrics.counter("devflow.worker.claimed").increment(batch.size());
        for (ClaimedJob claimed : batch) {
            execute(claimed);
        }
        Timer.builder("devflow.worker.batch.duration")
                .tag("result", batch.isEmpty() ? "empty" : "claimed")
                .register(metrics)
                .record(Duration.ofNanos(System.nanoTime() - batchStarted));
    }

    private void validateEvent(JobEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("Job event is required");
        }
        List<ConstraintViolation<JobEvent>> violations = new ArrayList<>(validator.validate(event));
        if (!violations.isEmpty()) {
            throw new IllegalArgumentException("Invalid job event: " + violations.get(0).getPropertyPath()
                    + " " + violations.get(0).getMessage());
        }
        if (!JobEvent.TYPE.equals(event.eventType())
                || event.schemaVersion() != JobEvent.CURRENT_SCHEMA_VERSION) {
            throw new IllegalArgumentException("Unsupported job event type or schema version");
        }
    }

    private List<ClaimedJob> claimBatch() {
        return transactions.execute(status -> {
            List<TenantQuota> quotas = quotaRepository.findAllForUpdate();
            Map<String, TenantQuota> quotaByTenant = new HashMap<>();
            quotas.forEach(quota -> quotaByTenant.put(quota.getTenantId(), quota));

            Instant now = clock.instant();
            List<JobEntity> candidates = jobRepository.findDispatchCandidates(
                    CLAIMABLE, now, PageRequest.of(0, workerProperties.getClaimBatchSize()));
            if (candidates.isEmpty()) {
                return List.of();
            }

            Map<String, Integer> runningByTenant = new HashMap<>();
            Map<String, Integer> runningResources = new HashMap<>();
            int runningCount = 0;
            for (JobRepository.RunningJobUsage usage : jobRepository.findRunningUsageByTenant()) {
                int tenantRunningCount = Math.toIntExact(usage.getRunningCount());
                int tenantRunningResources = Math.toIntExact(usage.getRunningResourceUnits());
                runningByTenant.put(usage.getTenantId(), tenantRunningCount);
                runningResources.put(usage.getTenantId(), tenantRunningResources);
                runningCount = Math.addExact(runningCount, tenantRunningCount);
            }

            Map<String, Integer> resourceCaps = new HashMap<>();
            quotaByTenant.forEach((tenantId, quota) -> resourceCaps.put(tenantId, quota.getMaxResourceUnits()));
            for (JobEntity candidate : candidates) {
                if (!quotaByTenant.containsKey(candidate.getTenantId())) {
                    throw new IllegalStateException("Missing quota row for queued job tenant "
                            + candidate.getTenantId());
                }
            }
            List<JobRecord> records = candidates.stream()
                    .map(job -> new JobRecord(job.getId().toString(), job.getTenantId(), job.getJobType(),
                            job.getPriority(), job.getResourceUnits(), job.getCreatedAt(), job.getScheduledAt(),
                            Math.max(0, Duration.between(job.getCreatedAt(), now).getSeconds()),
                            job.getAttemptCount(), job.getStatus()))
                    .toList();
            List<JobRecord> selected = scheduler.schedule(records, runningCount, runningByTenant,
                    runningResources, resourceCaps);

            List<ClaimedJob> claimed = new ArrayList<>();
            for (JobRecord record : selected) {
                Long jobId = Long.valueOf(record.getId());
                JobEntity job = jobRepository.findByIdForUpdate(jobId)
                        .orElseThrow(() -> new IllegalStateException("Scheduled job disappeared: " + jobId));
                if (!job.getTenantId().equals(record.getTenantId())) {
                    throw new IllegalStateException("Scheduled job tenant changed during claim");
                }
                String token = UUID.randomUUID().toString();
                Instant expiresAt = now.plus(leaseDuration);
                if (job.claim(token, expiresAt, now)) {
                    jobRepository.save(job);
                    historyRepository.save(new JobExecutionHistory(job, JobStatus.RUNNING,
                            "Execution claimed; attempt " + job.getAttemptCount()));
                    claimed.add(new ClaimedJob(job.getId(), job.getTenantId(), job.getJobType(), job.getPayload(),
                            job.getCorrelationId(), token, job.getAttemptCount(), expiresAt, job.getCreatedAt()));
                    metrics.timer("devflow.worker.job.queue.age")
                            .record(Duration.between(job.getCreatedAt(), now).isNegative()
                                    ? Duration.ZERO : Duration.between(job.getCreatedAt(), now));
                }
            }
            return claimed;
        });
    }

    private void execute(ClaimedJob claimed) {
        long executionStarted = System.nanoTime();
        String outcome = "failed";
        activeExecutions.incrementAndGet();
        activeLeases.put(claimed.jobId(), new ActiveLease(claimed.jobId(), claimed.token()));
        MDC.put("jobId", claimed.jobId().toString());
        MDC.put("correlationId", claimed.correlationId());
        try {
            JobHandler handler = handlers.stream().filter(candidate -> candidate.supports(claimed.jobType()))
                    .findFirst().orElseThrow(() -> new IllegalStateException(
                            "No job handler registered for type " + claimed.jobType()));
            JobExecutionContext context = new JobExecutionContext(claimed.jobId(), claimed.tenantId(),
                    claimed.jobType(), claimed.payload(), claimed.correlationId(), claimed.token(),
                    claimed.attempt(), claimed.leaseExpiresAt());
            String result = handler.execute(context);
            outcome = complete(claimed, result) ? "succeeded" : "fenced";
        } catch (Exception ex) {
            outcome = fail(claimed, ex).metricValue();
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        } finally {
            metrics.timer("devflow.worker.job.processing", "outcome", outcome)
                    .record(Duration.ofNanos(System.nanoTime() - executionStarted));
            activeLeases.remove(claimed.jobId());
            activeExecutions.decrementAndGet();
            MDC.remove("jobId");
            MDC.remove("correlationId");
        }
    }

    private boolean complete(ClaimedJob claimed, String result) {
        Boolean completed = transactions.execute(status -> jobRepository.findByIdForUpdate(claimed.jobId())
                .map(job -> {
                    if (!job.complete(claimed.token(), result, clock.instant())) {
                        return false;
                    }
                    jobRepository.save(job);
                    historyRepository.save(new JobExecutionHistory(job, JobStatus.SUCCEEDED, "Execution completed"));
                    metrics.counter("devflow.worker.completed").increment();
                    return true;
                }).orElse(false));
        if (!Boolean.TRUE.equals(completed)) {
            metrics.counter("devflow.worker.fenced").increment();
            log.warn("Discarded result from an expired or superseded worker lease; jobId={}", claimed.jobId());
        }
        return Boolean.TRUE.equals(completed);
    }

    private ExecutionOutcome fail(ClaimedJob claimed, Exception failure) {
        String message = failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
        if (message.length() > 1000) {
            message = message.substring(0, 1000);
        }
        final String failureMessage = message;
        ExecutionOutcome outcome = transactions.execute(status -> jobRepository.findByIdForUpdate(claimed.jobId())
                .map(job -> {
            Instant now = clock.instant();
            Instant retryAt = now.plus(retryPolicy.delayForAttempt(claimed.attempt()));
            if (!job.fail(claimed.token(), failureMessage, retryAt, now)) {
                metrics.counter("devflow.worker.fenced").increment();
                return ExecutionOutcome.FENCED;
            }
            historyRepository.save(new JobExecutionHistory(job, JobStatus.FAILED, failureMessage));
            if (job.getStatus() == JobStatus.RETRY_SCHEDULED) {
                historyRepository.save(new JobExecutionHistory(job, JobStatus.RETRY_SCHEDULED,
                        "Retry scheduled for " + job.getScheduledAt()));
                metrics.counter("devflow.worker.retry_scheduled").increment();
            } else {
                historyRepository.save(new JobExecutionHistory(job, JobStatus.DEAD_LETTER,
                        "Maximum attempts exhausted"));
                metrics.counter("devflow.worker.dead_lettered").increment();
            }
            jobRepository.save(job);
            log.warn("Job execution failed; jobId={}, attempt={}, status={}, error={}",
                    job.getId(), claimed.attempt(), job.getStatus(), failureMessage);
            return job.getStatus() == JobStatus.RETRY_SCHEDULED
                    ? ExecutionOutcome.RETRY_SCHEDULED : ExecutionOutcome.DEAD_LETTER;
        }).orElse(ExecutionOutcome.FENCED));
        return outcome == null ? ExecutionOutcome.FENCED : outcome;
    }

    @Scheduled(fixedDelayString = "${devflow.worker.lease-heartbeat-ms:20000}")
    public void renewActiveLeases() {
        for (ActiveLease lease : activeLeases.values()) {
            transactions.executeWithoutResult(status -> jobRepository.findByIdForUpdate(lease.jobId())
                    .filter(job -> job.getStatus() == JobStatus.RUNNING
                            && lease.token().equals(job.getLeaseToken()))
                    .ifPresent(job -> {
                        job.renewLease(lease.token(), clock.instant().plus(leaseDuration), clock.instant());
                        jobRepository.save(job);
                    }));
        }
    }

    public int recoverExpiredLeases() {
        Integer recovered = transactions.execute(status -> {
            quotaRepository.findAllForUpdate();
            Instant now = clock.instant();
            List<JobEntity> expired = jobRepository.findExpiredLeasesForUpdate(now, PageRequest.of(0, 100));
            for (JobEntity job : expired) {
                String token = job.getLeaseToken();
                String reason = "Execution lease expired; previous worker may have been interrupted";
                Instant retryAt = now.plus(retryPolicy.delayForAttempt(Math.max(1, job.getAttemptCount())));
                if (job.recoverExpiredLease(token, reason, retryAt, now)) {
                    historyRepository.save(new JobExecutionHistory(job, JobStatus.FAILED, reason));
                    historyRepository.save(new JobExecutionHistory(job, job.getStatus(),
                            job.getStatus() == JobStatus.DEAD_LETTER
                                    ? "Maximum attempts exhausted after lease expiry"
                                    : "Retry scheduled after lease expiry"));
                    jobRepository.save(job);
                    metrics.counter("devflow.worker.lease_recovered").increment();
                }
            }
            return expired.size();
        });
        return recovered == null ? 0 : recovered;
    }

    public int scheduleDueRetries() {
        Integer scheduled = transactions.execute(status -> {
            quotaRepository.findAllForUpdate();
            Instant now = clock.instant();
            List<JobEntity> due = jobRepository.findDueRetriesForUpdate(now, PageRequest.of(0, 100));
            for (JobEntity job : due) {
                job.transitionTo(JobStatus.QUEUED);
                job.setScheduledAt(now);
                jobRepository.save(job);
                historyRepository.save(new JobExecutionHistory(job, JobStatus.QUEUED, "Retry is ready for dispatch"));
                persistDispatchEvent(job, now);
            }
            return due.size();
        });
        return scheduled == null ? 0 : scheduled;
    }

    private void persistDispatchEvent(JobEntity job, Instant now) {
        String eventId = UUID.randomUUID().toString();
        JobEvent event = new JobEvent(eventId, JobEvent.TYPE, JobEvent.CURRENT_SCHEMA_VERSION,
                job.getId(), job.getTenantId(), job.getCorrelationId(), now);
        try {
            outboxRepository.save(new OutboxEvent(eventId, event.eventType(), event.schemaVersion(),
                    event.jobId(), event.correlationId(), objectMapper.writeValueAsString(event), now));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Unable to serialize retry outbox event", ex);
        }
    }

    private record ClaimedJob(Long jobId, String tenantId, String jobType, String payload,
                              String correlationId, String token, int attempt, Instant leaseExpiresAt,
                              Instant createdAt) {
    }

    private record ActiveLease(Long jobId, String token) {
    }

    private enum ExecutionOutcome {
        RETRY_SCHEDULED("retry_scheduled"),
        DEAD_LETTER("dead_letter"),
        FENCED("fenced");

        private final String metricValue;

        ExecutionOutcome(String metricValue) {
            this.metricValue = metricValue;
        }

        String metricValue() {
            return metricValue;
        }
    }
}
