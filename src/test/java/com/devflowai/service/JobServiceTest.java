package com.devflowai.service;

import com.devflowai.api.dto.JobSubmissionRequest;
import com.devflowai.api.dto.JobResponse;
import com.devflowai.config.JobSchedulingConfig;
import com.devflowai.domain.JobEntity;
import com.devflowai.domain.OutboxEvent;
import com.devflowai.domain.TenantQuota;
import com.devflowai.job.JobPriority;
import com.devflowai.job.JobStatus;
import com.devflowai.repository.JobExecutionHistoryRepository;
import com.devflowai.repository.OutboxEventRepository;
import com.devflowai.repository.JobRepository;
import com.devflowai.repository.TenantQuotaRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.PageRequest;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest
@Import({JobService.class, JobSchedulingConfig.class, JacksonAutoConfiguration.class})
@TestPropertySource(properties = {
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.flyway.enabled=false"
})
class JobServiceTest {

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobExecutionHistoryRepository historyRepository;

    @Autowired
    private TenantQuotaRepository quotaRepository;

    @Autowired
    private OutboxEventRepository outboxRepository;

    @Autowired
    private JobService jobService;

    @BeforeEach
    void setUp() {
        clearPersistedData();
    }

    @AfterEach
    void tearDown() {
        clearPersistedData();
    }

    private void clearPersistedData() {
        historyRepository.deleteAll();
        outboxRepository.deleteAll();
        jobRepository.deleteAll();
        quotaRepository.deleteAll();
    }

    @Test
    void submitJobCreatesPersistedJobAndHistory() {
        JobSubmissionRequest request = new JobSubmissionRequest(
                "report-generation",
                "{\"reportId\": 42}",
                JobPriority.HIGH,
                2,
                3,
                "idempotent-001");

        var response = jobService.submitJob("tenant-a", request);

        assertNotNull(response.getId());
        assertEquals(JobStatus.QUEUED, response.getStatus());
        assertEquals("tenant-a", response.getTenantId());
        assertEquals(1, jobRepository.count());
        assertEquals(2, historyRepository.findByJobIdOrderByCreatedAtAsc(response.getId()).size());
        OutboxEvent event = outboxRepository.findAll().getFirst();
        assertEquals(response.getId(), event.getJobId());
        assertEquals(OutboxEvent.Status.PENDING, event.getStatus());
        assertEquals(0, event.getAttemptCount());
    }

    @Test
    void duplicateIdempotentSubmissionDoesNotCreateDuplicateJobs() {
        JobSubmissionRequest request = new JobSubmissionRequest(
                "report-generation",
                "{\"reportId\": 42}",
                JobPriority.HIGH,
                2,
                3,
                "duplicate-key");

        JobResponse r1 = jobService.submitJob("tenant-a", request);
        JobResponse r2 = jobService.submitJob("tenant-a", request);

        assertEquals(r1.getId(), r2.getId());
        assertEquals(1, jobRepository.count());
        assertEquals(1, outboxRepository.count());
    }

    @Test
    void idempotencyKeysAreScopedToEachTenant() {
        JobSubmissionRequest request = new JobSubmissionRequest(
                "report-generation", "{}", JobPriority.NORMAL, 1, 3, "shared-key");

        JobResponse tenantA = jobService.submitJob("tenant-a", request);
        JobResponse tenantB = jobService.submitJob("tenant-b", request);

        assertNotEquals(tenantA.getId(), tenantB.getId());
        assertEquals(2, jobRepository.count());
        assertEquals(2, outboxRepository.count());
        assertEquals("tenant-a", tenantA.getTenantId());
        assertEquals("tenant-b", tenantB.getTenantId());
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void concurrentDuplicateSubmissionsReturnTheSamePersistedJob() throws Exception {
        JobSubmissionRequest request = new JobSubmissionRequest(
                "task", "{}", JobPriority.NORMAL, 1, 3, "simultaneous-key");
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<JobResponse> first = executor.submit(() -> {
                start.await();
                return jobService.submitJob("tenant-a", request);
            });
            Future<JobResponse> second = executor.submit(() -> {
                start.await();
                return jobService.submitJob("tenant-a", request);
            });

            start.countDown();
            assertEquals(first.get().getId(), second.get().getId());
            assertEquals(1, jobRepository.countByTenantId("tenant-a"));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void listingAppliesTenantFiltersAndPagination() {
        JobEntity firstMatch = new JobEntity("tenant-a", "first", "{}", JobPriority.HIGH, 1, 3, "first");
        firstMatch.transitionTo(JobStatus.QUEUED);
        jobRepository.save(firstMatch);

        JobEntity secondMatch = new JobEntity("tenant-a", "second", "{}", JobPriority.HIGH, 1, 3, "second");
        secondMatch.transitionTo(JobStatus.QUEUED);
        jobRepository.save(secondMatch);

        JobEntity otherTenant = new JobEntity("tenant-b", "other", "{}", JobPriority.HIGH, 1, 3, "other");
        otherTenant.transitionTo(JobStatus.QUEUED);
        jobRepository.save(otherTenant);

        JobEntity wrongStatus = new JobEntity("tenant-a", "failed", "{}", JobPriority.HIGH, 1, 3, "failed");
        wrongStatus.transitionTo(JobStatus.QUEUED);
        wrongStatus.transitionTo(JobStatus.RUNNING);
        wrongStatus.transitionTo(JobStatus.FAILED);
        jobRepository.save(wrongStatus);

        var page = jobService.listJobs("tenant-a", JobStatus.QUEUED, JobPriority.HIGH, PageRequest.of(0, 1));

        assertEquals(2, page.getTotalElements());
        assertEquals(2, page.getTotalPages());
        assertEquals(1, page.getContent().size());
        assertEquals("tenant-a", page.getContent().get(0).getTenantId());
        assertEquals(JobStatus.QUEUED, page.getContent().get(0).getStatus());
    }

    @Test
    void anotherTenantCannotRetrieveJobOrHistory() {
        JobResponse job = jobService.submitJob("tenant-a",
                new JobSubmissionRequest("task", "{}", JobPriority.NORMAL, 1, 2, "tenant-key"));

        assertThrows(JobNotFoundException.class, () -> jobService.getJobById("tenant-b", job.getId()));
        assertThrows(JobNotFoundException.class, () -> jobService.getExecutionHistory("tenant-b", job.getId()));
    }

    @Test
    void cancelEligibleJobChangesState() {
        JobSubmissionRequest request = new JobSubmissionRequest("task", "{}", JobPriority.NORMAL, 1, 2, "cancel-key");
        var response = jobService.submitJob("tenant-a", request);

        var cancelled = jobService.cancelJob("tenant-a", response.getId());

        assertEquals(JobStatus.CANCELLED, cancelled.getStatus());
        assertEquals(3, historyRepository.findByJobIdOrderByCreatedAtAsc(response.getId()).size());
    }

    @Test
    void retryOnlyFailedJobs() {
        JobEntity job = new JobEntity("tenant-a", "task", "{}", JobPriority.NORMAL, 1, 2, "retry-key");
        job.transitionTo(JobStatus.QUEUED);
        job.transitionTo(JobStatus.RUNNING);
        job.transitionTo(JobStatus.FAILED);
        job.incrementAttemptCount();
        job = jobRepository.save(job);

        var retried = jobService.retryJob("tenant-a", job.getId());

        assertEquals(JobStatus.RETRY_SCHEDULED, retried.getStatus());
        assertEquals(2, retried.getAttemptCount());
    }

    @Test
    void quotaViolationRejectsAdditionalJobs() {
        quotaRepository.save(new TenantQuota("tenant-a", 1, 10));
        jobRepository.save(new JobEntity("tenant-a", "task", "{}", JobPriority.NORMAL, 1, 2, "one"));
        var job = new JobEntity("tenant-a", "task2", "{}", JobPriority.NORMAL, 1, 2, "two");
        job.transitionTo(JobStatus.QUEUED);
        jobRepository.save(job);

        JobSubmissionRequest request = new JobSubmissionRequest("task3", "{}", JobPriority.NORMAL, 1, 2, "three");
        assertThrows(JobSubmissionException.class, () -> jobService.submitJob("tenant-a", request));
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void concurrentSubmissionsCannotExceedAggregateResourceQuota() throws Exception {
        quotaRepository.save(new TenantQuota("tenant-a", 5, 5));
        JobSubmissionRequest firstRequest = new JobSubmissionRequest(
                "task-a", "{}", JobPriority.NORMAL, 3, 2, "aggregate-a");
        JobSubmissionRequest secondRequest = new JobSubmissionRequest(
                "task-b", "{}", JobPriority.NORMAL, 3, 2, "aggregate-b");
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> first = executor.submit(() -> submitAndReportAccepted(start, firstRequest));
            Future<Boolean> second = executor.submit(() -> submitAndReportAccepted(start, secondRequest));

            start.countDown();
            int accepted = (first.get() ? 1 : 0) + (second.get() ? 1 : 0);
            assertEquals(1, accepted);
            assertEquals(3, jobRepository.sumResourceUnitsByTenantAndStatusIn("tenant-a",
                    List.of(JobStatus.ACCEPTED, JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.RETRY_SCHEDULED)));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void jobExceedingTenantResourceCapIsRejected() {
        quotaRepository.save(new TenantQuota("tenant-a", 5, 4));
        JobSubmissionRequest request = new JobSubmissionRequest("task", "{}", JobPriority.NORMAL, 5, 2, "too-large");

        assertThrows(JobSubmissionException.class, () -> jobService.submitJob("tenant-a", request));
        assertEquals(0, jobRepository.count());
    }

    @Test
    void retryIsRejectedAfterMaximumAttempts() {
        JobEntity job = new JobEntity("tenant-a", "task", "{}", JobPriority.NORMAL, 1, 1, "max-retry");
        job.transitionTo(JobStatus.QUEUED);
        job.transitionTo(JobStatus.RUNNING);
        job.transitionTo(JobStatus.FAILED);
        job.incrementAttemptCount();
        job = jobRepository.save(job);
        Long jobId = job.getId();

        assertThrows(JobStateTransitionException.class, () -> jobService.retryJob("tenant-a", jobId));
        assertEquals(JobStatus.FAILED, jobRepository.findById(jobId).orElseThrow().getStatus());
    }

    private boolean submitAndReportAccepted(CountDownLatch start, JobSubmissionRequest request) throws Exception {
        start.await();
        try {
            jobService.submitJob("tenant-a", request);
            return true;
        } catch (JobSubmissionException ex) {
            return false;
        }
    }
}
