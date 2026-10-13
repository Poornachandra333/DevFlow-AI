# Testing and verification

## Local test suite

Use Java 21 and Maven from the repository root:

```powershell
mvn test
```

The complete suite includes Redis, MySQL, and Kafka integration tests. They start disposable Redis 7, MySQL 8.4, and Kafka 3.8.0 containers through Testcontainers 2.0.5 and clean up only test-created resources. They require a reachable Docker Engine. The `*IT` performance benchmark is run explicitly; it is not part of the default Surefire test selection.

## Focused reliability tests

```powershell
mvn "-Dtest=RedisRateLimitStoreIntegrationTest,RedisReadinessIntegrationTest,RedisRateLimitStoreUnavailableTest,ProductionTenantSecurityTest,JobExecutionServiceTest,OutboxDispatchServiceTest" test
mvn "-Dtest=WorkerPropertiesTest,KafkaWorkerConfigTest,JobExecutionServiceTest,JobLeaseTest,MySqlJobExecutionIntegrationTest,KafkaWorkerIntegrationTest" test
mvn "-Dtest=MySqlJobExecutionIntegrationTest" test
mvn "-Dtest=KafkaWorkerIntegrationTest" test
```

Other useful commands:

```powershell
mvn dependency:tree "-Dincludes=org.testcontainers"
mvn dependency:tree "-Dincludes=com.github.docker-java"
mvn "-Dtest=TenantSubmissionRateLimiterTest,JobSubmissionRateLimitInterceptorTest" test
```

## Testcontainers/Docker compatibility evidence

The project uses the Testcontainers 2.0.5 BOM for both `org.testcontainers:testcontainers` and `org.testcontainers:testcontainers-junit-jupiter`. Testcontainers 1.21.0 used docker-java 3.4.2 and previously failed against Docker Desktop with HTTP 400 and an empty Docker server description. Version 2.0.5 resolves docker-java API/transport 3.7.1. The upgraded client was observed connecting to Docker Desktop 4.63.0, Docker Engine 29.2.1, API 1.53, over the automatically detected Windows named pipe, and starting Redis 7. This strongly supports a Docker Java client/API compatibility mismatch as the reason for the old failure; the exact request that failed under 1.21.0 was not recorded.

Resolved dependency outputs:

- `mvn dependency:tree "-Dincludes=org.testcontainers"`: `testcontainers` 2.0.5 and `testcontainers-junit-jupiter` 2.0.5.
- The MySQL and Kafka integration modules also resolve through the Testcontainers 2.0.5 BOM: `testcontainers-mysql` 2.0.5 and `testcontainers-kafka` 2.0.5.
- `mvn dependency:tree "-Dincludes=com.github.docker-java"`: `docker-java-api`, `docker-java-transport`, and `docker-java-transport-zerodep` 3.7.1.

## Results recorded on 2026-10-13

| Command | Tests run | Passed | Failed | Errors | Skipped |
|---|---:|---:|---:|---:|---:|
| Focused reliability command above | 18 | 18 | 0 | 0 | 0 |
| `mvn test` | 56 | 56 | 0 | 0 | 0 |
| Earlier `mvn "-Dtest=MySqlJobExecutionIntegrationTest" test` | 3 | 3 | 0 | 0 | 0 |
| First `mvn "-Dtest=MySqlJobExecutionIntegrationTest" test` with distinct-job case | 4 | 3 | 1 | 0 | 0 |
| First `mvn "-Dtest=KafkaWorkerIntegrationTest" test` | 2 | 1 | 0 | 1 | 0 |
| Latest `mvn "-Dtest=MySqlJobExecutionIntegrationTest" test` | 4 | 4 | 0 | 0 | 0 |
| `mvn "-Dtest=KafkaWorkerIntegrationTest" test` | 2 | 2 | 0 | 0 | 0 |
| Updated focused worker/config/MySQL/Kafka command above, including `JobLeaseTest` | 21 | 21 | 0 | 0 | 0 |
| Earlier `mvn test` before MySQL claim fix | 65 | 65 | 0 | 0 | 0 |
| `mvn "-Dtest=OutboxKafkaMySqlIntegrationTest,MySqlJobExecutionIntegrationTest,KafkaWorkerIntegrationTest,JobExecutionServiceTest,JobLeaseTest,OutboxDispatchServiceTest" test` | 26 | 26 | 0 | 0 | 0 |
| `mvn "-Dtest=JobProcessingPerformanceBaselineIT" test` | 1 | 1 | 0 | 0 | 0 |
| Earlier `mvn "-Dtest=WorkerProcessRecoveryIntegrationTest" test` | 4 | 4 | 0 | 0 | 0 |
| `mvn "-Dtest=WorkerProcessRecoveryIntegrationTest#twoInstancesSustainConcurrentSubmissionsClaimsRetriesAndDuplicateKafkaEvents" test` | 1 | 1 | 0 | 0 | 0 |
| Latest `mvn "-Dtest=JobProcessingPerformanceBaselineIT" test` | 1 | 1 | 0 | 0 | 0 |
| Latest `mvn "-Dtest=WorkerProcessRecoveryIntegrationTest#unrelatedKafkaEventCannotClaimDueRetryBeforeTransactionalOutboxScheduling" test` | 1 | 1 | 0 | 0 | 0 |
| Latest `mvn "-Dtest=WorkerProcessRecoveryIntegrationTest" test` | 5 | 5 | 0 | 0 | 0 |
| Latest `mvn test` | 78 | 78 | 0 | 0 | 0 |

The earlier focused run had one failure: `/actuator/health/readiness` returned 404 when only the `production` profile was active. Probe enablement was scoped to `application-rate-limit.yml`; it was moved to base `application.yml`. The first MySQL distinct-job concurrency run had one failure: the second job remained `QUEUED`. Direct checks showed both candidates were eligible; `JobEntity.claim` had an inconsistent time guard on `QUEUED` records, so the guard now applies only to `RETRY_SCHEDULED`. The first Kafka iteration had one error because its fixture created two quota rows for one tenant; its marker job now uses a unique tenant. Handler ordering was also made explicit so the test-specific handler precedes the catch-all fallback. The final focused reruns and full suite passed. No test is represented as passed when skipped; the latest Surefire summary reports zero skipped tests.

## Behaviors verified

- Redis Lua fixed-window counter increments atomically; the test observes the TTL and verifies the key expires and the count resets.
- Forty concurrent Redis increments are not lost.
- The live rate limiter enforces its configured per-tenant bound, isolates tenant keys, and produces a 429 response with a positive `Retry-After`.
- A safe connection-refusal test uses a just-released local loopback port and confirms the limiter fails closed with `RateLimitStoreUnavailableException`. The interceptor/exception-handler tests cover the corresponding 503 response mapping.
- The production readiness probe reports `UP` while the required profile-gated Redis dependency is healthy and returns `DOWN`/503 after the test-owned Redis container is stopped.
- Production readiness probes are accessible without application credentials; protected job API requests remain unauthorized without a trusted authenticated principal.
- Worker MDC contains job/correlation identifiers during handler execution and is cleared afterward. The outbox failure test verifies event ID MDC is present during publication, cleared after failure, and that retry metadata is persisted.
- Existing H2 tests exercise tenant-scoped API operations, duplicate event handling, scheduler claim bounds, retry/dead-letter transitions, lease recovery, and duplicate worker claims. These do not establish correctness under real MySQL multi-instance locking.
- Worker metrics include `devflow.worker.job.processing` (finite `outcome` values), `devflow.worker.job.queue.age`, `devflow.worker.claim.batch.size`, `devflow.worker.batch.duration` (finite `result` values), `devflow.worker.claimed`, and `devflow.worker.active`, alongside existing retry/recovery counters. Job, tenant, correlation, and event IDs are not metric labels.
- Worker claim capacity is configured through `devflow.worker.claim-batch-size` / `DEVFLOW_WORKER_CLAIM_BATCH_SIZE`; Kafka listener concurrency is independently configured through `devflow.worker.concurrency` / `DEVFLOW_WORKER_CONCURRENCY`. Both are validated to be at least 1. Scheduler-wide limits can still lower the number actually selected.
- Real MySQL 8.4 tests run Flyway migrations, verify persisted job/outbox creation and idempotent resubmission, verify transaction rollback removes the job and outbox record, concurrently deliver one duplicate event to two consumers, and concurrently deliver two distinct job events to confirm both jobs are claimed.
- Real Kafka 3.8.0 tests publish duplicate events through the broker and verify one execution, then send an invalid persisted-job reference and observe it retry to the configured `.DLT` topic.
- `mvn "-Dtest=WorkerProcessRecoveryIntegrationTest" test` passed four tests against disposable MySQL 8.4 and Kafka 3.8.0. A child JVM is killed forcibly after it has claimed a job and blocked inside its handler; the test then marks its lease expired in MySQL, starts a replacement Spring context, recovers and retries the job, and proves the old lease token cannot complete it. A second child JVM is killed after the real Kafka publisher returns its broker acknowledgement but before the outbox-delivery transaction; an independent dispatcher reclaims the manually expired lease and republishes. The test observes both broker records and repeated delivery, while the persisted job reaches one successful attempt. No external side effect is exercised, so exactly-once effects are not established.
- The same integration class runs two independent application contexts against shared disposable MySQL 8.4/Kafka 3.8.0 with a two-partition topic. It submits 47 normal jobs across 12 tenants using eight submission threads and separately exercises a deterministic retry job. The regression test makes a retry due while an unrelated Kafka event is consumed, then verifies the retry remains `RETRY_SCHEDULED` until the retry poller atomically queues it with a matching outbox row. Two instances race the scheduling operation and the consumer claim; only one scheduling transition and one retry execution occur. The scenario also covers duplicate Kafka delivery, consumer-group catch-up, and retry exhaustion to `DEAD_LETTER`. The larger contention workload sends 24 duplicate Kafka records. This is short integration contention, not production-scale or long-duration stress, and does not establish external-effect idempotency.
- The original defect was that `claimBatch()` included due `RETRY_SCHEDULED` jobs among globally dispatchable candidates. Any unrelated Kafka event could therefore claim such a retry before `scheduleDueRetries()` performed its transactionally paired `RETRY_SCHEDULED` → `QUEUED` transition and retry outbox insert. Claim eligibility is now limited to `QUEUED`; due retries must pass through the poller/outbox transaction before they can be claimed. This does not change retry policy or delivery semantics.
- Delivery is at-least-once, not exactly-once. The duplicate publication scenario verifies a persisted job executes once under the tested conditions; external handler side effects can still repeat after crash windows and must be idempotent.
- Worker metrics are exercised through success/failure and the combined integration scenario; metric tags use finite outcome/result values and exclude tenant/job/request identifiers.
- Retry-race regression verification on 2026-10-13: `mvn "-Dtest=WorkerProcessRecoveryIntegrationTest#unrelatedKafkaEventCannotClaimDueRetryBeforeTransactionalOutboxScheduling" test` — 1 passed, 0 failed, 0 errors, 0 skipped; `mvn "-Dtest=WorkerProcessRecoveryIntegrationTest" test` — 5 passed, 0 failed, 0 errors, 0 skipped; `mvn test` — 78 passed, 0 failed, 0 errors, 0 skipped. All integration tests in these runs used disposable Testcontainers infrastructure. During test development, an initial focused run exposed timestamp-tie ambiguity in selecting the newly inserted outbox event; the test now selects by excluding the original event ID. An initial recovery-class run also exposed that its consumer-group offset check could precede confirmation that the retry event was delivered; the test now waits for that specific outbox row to reach `DELIVERED` before waiting for group catch-up. The final focused, class, and full-suite runs passed.

## Performance baseline

Run the benchmark independently of `mvn test`:

```powershell
mvn "-Dtest=JobProcessingPerformanceBaselineIT" test
```

The latest benchmark run on 2026-10-13 passed 1 test (0 failures, 0 errors, 0 skipped). Environment/workload: Java 21.0.10, MySQL 8.4, 1,200 running jobs, 250 queued jobs, 60 submissions, 2 worker warm-up batches, and 8 measured worker batches. Results from that run:

| Workload | Operations | SQL statements / entity loads | Throughput | p50 | p95 |
|---|---:|---:|---:|---:|---:|
| Candidate query | 12 queries | 12 statements / 300 entity loads | — | 5 ms | 6 ms |
| Submission | 60 submissions | 660 statements / 0 entity loads | 30 ops/s | 43 ms | 58 ms |
| Worker claim/execution | 200 jobs, 8 batches | 1,540 statements / 1,120 entity loads; 770 query executions | 40 jobs/s | 658 ms/batch | 713 ms/batch |

The worker-usage query comparison ran 12 repetitions per query shape against the same 1,200-row `RUNNING` fixture. The previous full-entity load prepared 12 statements, materialized 14,400 entities, and measured p50/p95 37/70 ms. The grouped tenant aggregate prepared 12 statements, materialized zero entities, and measured p50/p95 8/15 ms. This is a within-run query-shape comparison, not a Git-history comparison: this checkout has no commit baseline.

The test printed these MySQL `EXPLAIN` plans: both running-usage query shapes used `idx_jobs_status_priority`; the grouped aggregate reported `Using temporary`. The job candidate query used range access via `idx_jobs_dispatch` (estimated 61 rows) and reported `Using where; Using index; Using filesort`; the outbox query used a full scan (estimated 60 rows) and `Using where; Using filesort`. The outbox fixture is small; these plans do not establish a production-scale bottleneck or justify adding an index.

An earlier run of the same seeded workload before replacing the worker's full `RUNNING` entity load with a per-tenant aggregate measured 13,120 worker entity loads, 20 jobs/s, and 1,277 ms p50 / 1,362 ms p95 batch latency. The first post-change run measured 1,120 worker entity loads, 33 jobs/s, and 771 ms / 899 ms p50/p95. The final repeat retained 1,120 worker entity loads but measured 25 jobs/s and 1,074 ms / 1,124 ms. These timings vary between runs and are environment-specific; the repeated data supports reduced entity materialization, but does not establish a stable throughput or latency improvement percentage.

## Not verified / remaining limits

- These are real disposable Testcontainers runs, not H2 substitutes. Process-crash tests use separate JVMs at deterministic claim and post-acknowledgement barriers, then recover using new Spring contexts. The tests manually expire the corresponding leases in MySQL; they do not wait for natural lease expiry or test an external process supervisor. The 48-job two-context test is short-lived and does not prove general multi-instance correctness or production locking behavior.
- Flyway resolves as 11.7.2 (`flyway-core` and `flyway-mysql`). Its runtime warning states MySQL 8.4 is newer than its tested support ceiling, MySQL 8.1; the version-pinned [Flyway 11.7.2 MySQL compatibility source](https://github.com/flyway/flyway/blob/flyway-11.7.2/flyway-database/flyway-mysql/src/main/java/org/flywaydb/database/mysql/MySQLDatabase.java) contains that ceiling. Both project migrations executed successfully on disposable MySQL 8.4 in this suite; this is observed compatibility, not a vendor support guarantee. MySQL 8.4 is therefore project-tested but outside Flyway 11.7.2's stated tested support range. No Flyway version was changed.
- Redis was verified against a disposable local Redis container, but Redis failover, network partition recovery, and production topology are not covered.
- The performance baseline is repeatable in setup and workload but consists of a small number of environment-sensitive measurements; sustained load, long-run scheduling fairness, and production-sized outbox behavior remain unverified.
- No Docker/Compose startup test, CI run, cloud deployment, or deployment cost evaluation was performed.
- Latest verification on 2026-10-13: the targeted two-instance retry/outbox test passed 1/1, the complete process-recovery class passed 4/4, and `mvn test` passed 77/77 with 0 failures, 0 errors, and 0 skipped. A previous full-class run exposed a timing issue in the test's retry-event assertion; the test now waits for the consumer group to catch up before scheduling the isolated retry. No production code changed in this follow-up.
