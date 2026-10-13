package com.devflowai.performance;

import com.devflowai.api.dto.JobSubmissionRequest;
import com.devflowai.domain.JobEntity;
import com.devflowai.domain.TenantQuota;
import com.devflowai.event.JobEvent;
import com.devflowai.job.JobPriority;
import com.devflowai.job.JobStatus;
import com.devflowai.repository.JobRepository;
import com.devflowai.repository.TenantQuotaRepository;
import com.devflowai.service.JobExecutionService;
import com.devflowai.service.JobService;
import jakarta.persistence.EntityManagerFactory;
import org.hibernate.SessionFactory;
import org.hibernate.stat.Statistics;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
@SpringBootTest(properties = {
        "devflow.scheduling.max-concurrent-jobs=2000",
        "devflow.scheduling.max-concurrent-per-tenant=2000",
        "devflow.worker.claim-batch-size=25",
        "spring.jpa.properties.hibernate.generate_statistics=true"
})
@ActiveProfiles("production")
class JobProcessingPerformanceBaselineIT {
    private static final int RUNNING_JOBS = 1200;
    private static final int QUEUED_JOBS = 250;
    private static final int SUBMISSIONS = 60;
    private static final int WARMUP_BATCHES = 2;
    private static final int MEASURED_BATCHES = 8;

    @Container
    private static final MySQLContainer MYSQL =
            new MySQLContainer(DockerImageName.parse("mysql:8.4"))
                    .withDatabaseName("devflow_performance")
                    .withUsername("devflow")
                    .withPassword("devflow-test-only");

    @Autowired
    private JobService jobService;
    @Autowired
    private JobRepository jobRepository;
    @Autowired
    private TenantQuotaRepository quotaRepository;
    @Autowired
    private JobExecutionService executionService;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private EntityManagerFactory entityManagerFactory;

    @DynamicPropertySource
    static void configureMySql(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.datasource.driver-class-name", MYSQL::getDriverClassName);
    }

    @Test
    void measuresSubmissionCandidateQueryAndWorkerClaimWorkload() {
        String tenant = "perf-worker-" + UUID.randomUUID();
        quotaRepository.saveAndFlush(new TenantQuota(tenant, 2000, 2000));
        seedRunningJobs(tenant, RUNNING_JOBS);
        List<JobEntity> queued = seedQueuedJobs(tenant, QUEUED_JOBS);

        Statistics statistics = entityManagerFactory.unwrap(SessionFactory.class).getStatistics();
        statistics.clear();

        measureRunningUsageAlternatives(statistics);
        List<Long> candidateQueryNanos = measureCandidateQueries(statistics);
        long candidateQueryCount = statistics.getPrepareStatementCount();
        long candidateEntityLoads = statistics.getEntityLoadCount();
        long candidateQueryExecutions = statistics.getQueryExecutionCount();

        List<Long> submissionNanos = measureSubmissions();
        long submissionStatementCount = statistics.getPrepareStatementCount();
        long submissionEntityLoads = statistics.getEntityLoadCount();

        JobEntity eventJob = queued.get(0);
        JobEvent event = new JobEvent(UUID.randomUUID().toString(), JobEvent.TYPE,
                JobEvent.CURRENT_SCHEMA_VERSION, eventJob.getId(), eventJob.getTenantId(),
                eventJob.getCorrelationId(), Instant.now());
        List<Long> batchNanos = new ArrayList<>();
        long jobsExecuted = 0;
        for (int batch = 0; batch < WARMUP_BATCHES + MEASURED_BATCHES; batch++) {
            long started = System.nanoTime();
            executionService.consume(event);
            long duration = System.nanoTime() - started;
            if (batch >= WARMUP_BATCHES) {
                batchNanos.add(duration);
                jobsExecuted += Math.min(25, QUEUED_JOBS - (batch * 25));
            }
        }
        long workerStatementCount = statistics.getPrepareStatementCount();
        long workerEntityLoads = statistics.getEntityLoadCount();
        long workerQueryExecutions = statistics.getQueryExecutionCount();
        reportQueryPlans();

        System.out.println("PERFORMANCE_BASELINE environment="
                + "java=" + System.getProperty("java.version")
                + ",mysql=" + MYSQL.getDockerImageName()
                + ",datasetRunning=" + RUNNING_JOBS
                + ",datasetQueued=" + QUEUED_JOBS
                + ",submissionOperations=" + SUBMISSIONS
                + ",warmupBatches=" + WARMUP_BATCHES
                + ",measuredBatches=" + MEASURED_BATCHES);
        System.out.println("PERFORMANCE_BASELINE candidateQuery"
                + " count=" + candidateQueryExecutions
                + " statements=" + candidateQueryCount
                + " entityLoads=" + candidateEntityLoads
                + " p50_ms=" + percentileMillis(candidateQueryNanos, 50)
                + " p95_ms=" + percentileMillis(candidateQueryNanos, 95));
        System.out.println("PERFORMANCE_BASELINE submission"
                + " operations=" + SUBMISSIONS
                + " statements=" + (submissionStatementCount - candidateQueryCount)
                + " entityLoads=" + (submissionEntityLoads - candidateEntityLoads)
                + " throughput_ops_per_sec=" + throughput(SUBMISSIONS, submissionNanos)
                + " p50_ms=" + percentileMillis(submissionNanos, 50)
                + " p95_ms=" + percentileMillis(submissionNanos, 95));
        System.out.println("PERFORMANCE_BASELINE worker"
                + " jobsExecuted=" + jobsExecuted
                + " batches=" + MEASURED_BATCHES
                + " statements=" + (workerStatementCount - submissionStatementCount)
                + " entityLoads=" + (workerEntityLoads - submissionEntityLoads)
                + " throughput_jobs_per_sec=" + throughput(jobsExecuted, batchNanos)
                + " batch_p50_ms=" + percentileMillis(batchNanos, 50)
                + " batch_p95_ms=" + percentileMillis(batchNanos, 95)
                + " queryExecutions=" + (workerQueryExecutions - candidateQueryExecutions));

        assertEquals(QUEUED_JOBS, jobRepository.countByTenantIdAndStatus(tenant, JobStatus.SUCCEEDED));
    }

    private void reportQueryPlans() {
        System.out.println("PERFORMANCE_BASELINE explain.running_old="
                + jdbcTemplate.queryForList("""
                EXPLAIN SELECT * FROM jobs WHERE status = 'RUNNING'
                """));
        System.out.println("PERFORMANCE_BASELINE explain.running_grouped="
                + jdbcTemplate.queryForList("""
                EXPLAIN SELECT tenant_id, COUNT(*), COALESCE(SUM(resource_units), 0)
                FROM jobs WHERE status = 'RUNNING' GROUP BY tenant_id
                """));
        System.out.println("PERFORMANCE_BASELINE explain.jobs="
                + jdbcTemplate.queryForList("""
                EXPLAIN SELECT id FROM jobs
                WHERE status IN ('QUEUED', 'RETRY_SCHEDULED')
                  AND (status <> 'RETRY_SCHEDULED' OR scheduled_at <= UTC_TIMESTAMP())
                ORDER BY created_at ASC
                LIMIT 25
                """));
        System.out.println("PERFORMANCE_BASELINE explain.outbox="
                + jdbcTemplate.queryForList("""
                EXPLAIN SELECT event_id FROM outbox_events
                WHERE (status = 'PENDING' AND next_attempt_at <= UTC_TIMESTAMP())
                   OR (status = 'IN_FLIGHT' AND lease_expires_at <= UTC_TIMESTAMP())
                ORDER BY created_at ASC
                LIMIT 100
                """));
    }

    private void measureRunningUsageAlternatives(Statistics statistics) {
        for (int i = 0; i < 3; i++) {
            jobRepository.findByStatus(JobStatus.RUNNING);
            jobRepository.findRunningUsageByTenant();
        }

        statistics.clear();
        List<Long> entityQueryNanos = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            long started = System.nanoTime();
            List<JobEntity> running = jobRepository.findByStatus(JobStatus.RUNNING);
            entityQueryNanos.add(System.nanoTime() - started);
            assertEquals(RUNNING_JOBS, running.size());
        }
        long entityStatements = statistics.getPrepareStatementCount();
        long entityLoads = statistics.getEntityLoadCount();

        statistics.clear();
        List<Long> groupedQueryNanos = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            long started = System.nanoTime();
            List<JobRepository.RunningJobUsage> usage = jobRepository.findRunningUsageByTenant();
            groupedQueryNanos.add(System.nanoTime() - started);
            assertEquals(1, usage.size());
            assertEquals(RUNNING_JOBS, usage.get(0).getRunningCount());
        }
        long groupedStatements = statistics.getPrepareStatementCount();
        long groupedEntityLoads = statistics.getEntityLoadCount();
        long groupedQueryExecutions = statistics.getQueryExecutionCount();

        System.out.println("PERFORMANCE_BASELINE runningUsage oldEntityQuery"
                + " queries=12 statements=" + entityStatements
                + " entityLoads=" + entityLoads
                + " p50_ms=" + percentileMillis(entityQueryNanos, 50)
                + " p95_ms=" + percentileMillis(entityQueryNanos, 95));
        System.out.println("PERFORMANCE_BASELINE runningUsage groupedAggregate"
                + " queries=" + groupedQueryExecutions
                + " statements=" + groupedStatements
                + " entityLoads=" + groupedEntityLoads
                + " p50_ms=" + percentileMillis(groupedQueryNanos, 50)
                + " p95_ms=" + percentileMillis(groupedQueryNanos, 95));
    }

    private void seedRunningJobs(String tenant, int count) {
        Instant now = Instant.now();
        List<JobEntity> jobs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            JobEntity job = new JobEntity(tenant, "echo", "{}", JobPriority.NORMAL,
                    1, 3, UUID.randomUUID().toString());
            job.transitionTo(JobStatus.QUEUED);
            job.setScheduledAt(now);
            job.claim(UUID.randomUUID().toString(), now.plus(Duration.ofHours(1)), now);
            jobs.add(job);
        }
        jobRepository.saveAllAndFlush(jobs);
    }

    private List<JobEntity> seedQueuedJobs(String tenant, int count) {
        List<JobEntity> jobs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            JobEntity job = new JobEntity(tenant, "echo", "{}", JobPriority.NORMAL,
                    1, 3, UUID.randomUUID().toString());
            job.transitionTo(JobStatus.QUEUED);
            job.setScheduledAt(Instant.now());
            jobs.add(job);
        }
        return jobRepository.saveAllAndFlush(jobs);
    }

    private List<Long> measureCandidateQueries(Statistics statistics) {
        for (int i = 0; i < 3; i++) {
            jobRepository.findDispatchCandidates(
                    List.of(JobStatus.QUEUED, JobStatus.RETRY_SCHEDULED),
                    Instant.now(), org.springframework.data.domain.PageRequest.of(0, 25));
        }
        statistics.clear();
        List<Long> durations = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            long started = System.nanoTime();
            var candidates = jobRepository.findDispatchCandidates(
                    List.of(JobStatus.QUEUED, JobStatus.RETRY_SCHEDULED),
                    Instant.now(), org.springframework.data.domain.PageRequest.of(0, 25));
            durations.add(System.nanoTime() - started);
            assertEquals(25, candidates.size());
        }
        return durations;
    }

    private List<Long> measureSubmissions() {
        List<Long> durations = new ArrayList<>();
        for (int i = 0; i < SUBMISSIONS; i++) {
            String tenant = "perf-submit-" + UUID.randomUUID();
            JobSubmissionRequest request = new JobSubmissionRequest();
            request.setJobType("echo");
            request.setPayload("{}");
            request.setPriority(JobPriority.NORMAL);
            request.setResourceUnits(1);
            request.setMaxAttempts(3);
            request.setIdempotencyKey(UUID.randomUUID().toString());
            long started = System.nanoTime();
            jobService.submitJob(tenant, request);
            durations.add(System.nanoTime() - started);
        }
        return durations;
    }

    private static long percentileMillis(List<Long> values, int percentile) {
        List<Long> sorted = values.stream().sorted(Comparator.naturalOrder()).toList();
        int index = Math.min(sorted.size() - 1,
                (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1);
        return TimeUnit.NANOSECONDS.toMillis(sorted.get(index));
    }

    private static long throughput(long operations, List<Long> samples) {
        long totalNanos = samples.stream().mapToLong(Long::longValue).sum();
        return TimeUnit.NANOSECONDS.toSeconds(totalNanos) == 0
                ? (long) (operations * 1_000_000_000.0 / totalNanos)
                : operations / TimeUnit.NANOSECONDS.toSeconds(totalNanos);
    }
}
