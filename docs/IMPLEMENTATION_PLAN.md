# Implementation Plan

## Scope and current position

The Java 21 / Spring Boot job platform contains the foundation, tenant-aware API/persistence, and distributed worker primitives. The latest complete Maven suite passes **78 tests**, including disposable Redis, MySQL, and Kafka Testcontainers integrations. The process-recovery/contention tests and the seeded MySQL performance benchmark also pass. These results verify only the scenarios those tests exercise; they are not proof of production-scale or general distributed correctness.

The roadmap below preserves the requested phase numbers. Phases 1–3 are implemented with the documented infrastructure limits. Phase 4 now includes disposable-infrastructure tests that forcibly terminate separate worker/dispatcher JVMs at specific crash windows, recover work in replacement contexts, fence a stale worker, and exercise 48 jobs across two contexts. Delivery remains at-least-once, and broad multi-instance guarantees are unproven. Phase 5 includes Redis readiness/rate-limit verification, bounded-cardinality worker metrics, independently configurable worker claim capacity and Kafka listener concurrency, and dependency-failure checks. Phase 5 and Phases 6–8 are not declared complete; longer-duration failover/topology behavior and deployment readiness remain gaps.

## Compatibility research

- The project pins Spring Boot 3.5.0 and Java 21 in `pom.xml`. Java 21 is within the Spring Boot 3.5 Java baseline.
- Spring AI's current reference publishes a 2.0.0 BOM and describes a changed `ChatClient` API. Do not add the current major as a drop-in to this Boot 3.5 application; before Phase 6, confirm a Spring AI line compatible with the selected Boot line and add only the required model/vector-store modules.
- Local validation must continue to work without MySQL, Kafka, Redis, or model-provider credentials. External services should be profile/configuration gated and tested with deterministic mocks where useful.
- Research references: [Spring Boot 3.5 system requirements](https://docs.spring.io/spring-boot/3.5/system-requirements.html), [Spring AI getting started](https://docs.spring.io/spring-ai/reference/getting-started.html), [Spring AI upgrade notes](https://docs.spring.io/spring-ai/reference/upgrade-notes.html).

## Existing phases

### Phase 1: Foundation — complete

- Java 21 Maven/Spring Boot application and core job/scheduling types exist.
- Request validation and scheduling contract tests exist.
- Latest `mvn test` completed successfully on 2026-10-13: 78 tests run, 78 passed, 0 failures, 0 errors, 0 skipped.

### Phase 2: API and persistence — implemented

- Tenant-scoped submission, retrieval, listing, history, cancellation, and retry endpoints exist.
- JPA persistence, Flyway MySQL migrations, idempotency, quotas, and lifecycle history exist.
- H2/MockMvc coverage exists. Disposable MySQL 8.4 tests now verify both migrations, transactional persistence/rollback, duplicate delivery, and concurrent claims for distinct jobs in one process; Flyway's tested support ceiling remains 8.1 and multi-instance behavior remains unverified.
- Production identity-provider integration is still absent; non-development profiles fail closed unless a trusted tenant principal is supplied.

### Phase 3: Scheduling — implemented

- Priority/fairness policy, tenant/resource admission, bounded retries, and starvation aging exist.
- Unit coverage exists; long-running fairness and performance benchmarks remain outstanding.

## Phase 4: Distributed execution

**Status:** Kafka listener, transactional outbox, retry/DLT configuration, worker leases, lease recovery, fencing, and duplicate-claim protection exist. Each event executes one scheduler claim batch. Real disposable Kafka and MySQL integration tests pass for outbox publication/acknowledgement and persisted delivery state, broker pause/recovery, duplicate publication/reclaim, lease recovery/retry, DLT routing, and independent application instances. Separate worker and dispatcher JVMs were forcibly terminated at deterministic crash points; replacement contexts recovered the expired leases. A short two-context workload exercised concurrent submission, claims, duplicate events, tenant mismatch, and a retry. Delivery remains at-least-once; neither exactly-once side effects nor broad multi-instance correctness are established.

### Implementation steps

#### 4.1 Bound work per delivery

- [x] T001 [Plan:4.1] Process one scheduler claim batch per Kafka event in `src/main/java/com/devflowai/service/JobExecutionService.java`.
- [x] T002 [Plan:4.1] Test backlog progress across successive events in `src/test/java/com/devflowai/service/JobExecutionServiceTest.java`.
- [x] T003 [Plan:4.1] Document bounded delivery semantics in `README.md` and `docs/ARCHITECTURE.md`.

#### 4.2 Verify real broker and database behavior

- [x] T004 [Plan:4.2] Add dedicated Kafka/MySQL integration coverage and test dependencies in `pom.xml` and `src/test/java/com/devflowai/integration/`.
- [x] T005 [Plan:4.2] Verify outbox broker-ack crash/reclaim, forcibly killed worker recovery, stale-lease fencing, and a bounded two-context contention workload using disposable Kafka/MySQL; document deterministic manual lease expiry and remaining scope limits.
- [x] T006 [Plan:4.2] Record the actual process-crash, contention, benchmark, and database compatibility evidence in `TESTING.md` and `docs/ARCHITECTURE.md`.

## Phase 5: Reliability and operations

**Progress:** Tenant submission limiting is implemented behind the opt-in `rate-limit` profile. Testcontainers 2.0.5 runs Redis 7, MySQL 8.4, and Kafka 3.8.0 on the local Docker Engine 29.2.1/API 1.53. Live Redis tests verify atomic counting, fixed-window expiry/reset, concurrent increments, tenant isolation, configured limit enforcement, and the `429`/`Retry-After` response. An isolated refused local connection verifies fail-closed store behavior; the exception-handler test verifies the `503` mapping. A production readiness test verifies `UP` with Redis and `DOWN`/503 after its disposable container stops. Health probes are global; production security permits readiness/liveness subpaths without opening the protected job API. Logstash JSON includes job/correlation/event MDC, with tests for worker and outbox MDC cleanup. Worker metrics cover processing duration by finite outcome, queue age, claim batch size/duration, claimed count, active worker count, and existing retry/recovery outcomes. `devflow.worker.claim-batch-size` and Kafka listener `devflow.worker.concurrency` are independently configurable and validated as positive values. The MySQL concurrent distinct-job test found that `JobEntity.claim` incorrectly applied `scheduledAt` to `QUEUED` jobs; the readiness check now gates time only for `RETRY_SCHEDULED`. Redis failover/topology, sustained fault recovery, and production-scale dependency behavior remain unverified; Phase 5 is not declared complete.

### Implementation steps

#### 5.1 Rate limiting and admission backpressure

- [x] T007 [Plan:5.1] Add tenant-scoped Redis rate limiting for job submission using atomic Redis operations in `src/main/java/com/devflowai/`.
- [x] T008 [Plan:5.1] Return a structured 429 response and retry guidance through `src/main/java/com/devflowai/api/JobApiExceptionHandler.java`.
- [x] T009 [Plan:5.1] Test quota boundaries, concurrent requests, and Redis outage behavior without requiring live Redis in the default suite.
- [x] T010 [Plan:5.1] Make worker claim batch capacity and Kafka listener concurrency explicit independent configuration in `src/main/resources/application.yml` and `src/main/resources/application-kafka.yml`; both values are validated as at least 1. API rate-limit settings remain profile/configuration gated.

#### 5.2 Health, metrics, recovery, and logging

- [x] T011 [Plan:5.2] Add operational health indicators and readiness behavior for enabled dependencies in `src/main/java/com/devflowai/config/`.
- [x] T012 [Plan:5.2] Add bounded-cardinality worker processing, queue-age, batch, claim, activity, retry, and recovery metrics in `src/main/java/com/devflowai/service/`. No job, tenant, correlation, or event identifiers are metric labels.
- [x] T013 [Plan:5.2] Add correlation-aware structured logging while excluding payloads, secrets, and tenant-sensitive data.
- [x] T014 [Plan:5.2] Test Redis dependency failure/readiness transitions, recovery, and concurrency, plus Kafka outage/recovery, MySQL lease/retry behavior, and bounded-cardinality worker metrics; remaining fault limits are documented below.

## Phase 6: GenAI and RAG

### Implementation steps

#### 6.1 Structured job proposals

- [ ] T015 [Plan:6.1] Add a validated proposal schema and natural-language proposal service in `src/main/java/com/devflowai/ai/`.
- [ ] T016 [Plan:6.1] Keep proposals separate from execution; require explicit user/API submission after deterministic validation.
- [ ] T017 [Plan:6.1] Add mock-provider tests and provider timeout, rate-limit, and malformed-response handling.

#### 6.2 Evidence-grounded troubleshooting

- [ ] T018 [Plan:6.2] Add a RAG retrieval boundary and diagnostic response model in `src/main/java/com/devflowai/ai/`.
- [ ] T019 [Plan:6.2] Restrict operational tools to read-only, allowlisted data and attach source evidence to diagnoses.
- [ ] T020 [Plan:6.2] Add provider-failure tests and a deterministic evaluation set for retrieval and diagnostic grounding.

## Phase 7: Verification and performance

### Implementation steps

#### 7.1 Regression, integration, and concurrency validation

- [x] T021 [Plan:7.1] Run the complete Maven test suite and fix regressions; latest run is 78 passed, 0 failed, 0 errors, 0 skipped.
- [x] T022 [Plan:7.1] Run focused Redis, MySQL, and Kafka integration tests, including separate-JVM process termination and 48-job/two-context contention against disposable services. Remaining untested scenarios are recorded below.
- [x] T023 [Plan:7.1] Document exact commands, counts, failures, infrastructure, and limitations in `README.md`, `TESTING.md`, and `docs/`.

#### 7.2 Benchmarks

- [x] T024 [Plan:7.2] Add a seeded MySQL scheduling/submission/worker benchmark with warm-up, repeated batches, SQL/entity counts, and `EXPLAIN` output in `src/test/java/com/devflowai/performance/JobProcessingPerformanceBaselineIT.java`.
- [x] T025 [Plan:7.2] Record actual workload, environment, repeated measurements, query plans, and limits in `TESTING.md`.

## Phase 8: Deployment readiness

### Implementation steps

#### 8.1 Local operations and CI

- [ ] T026 [Plan:8.1] Add Docker build and Compose configuration for local dependencies in `Dockerfile` and `compose.yaml`.
- [ ] T027 [Plan:8.1] Add CI build/test workflow in `.github/workflows/`.
- [ ] T028 [Plan:8.1] Validate clean local startup and graceful shutdown and document environment variables and placeholders.

#### 8.2 Deployment preparation

- [ ] T029 [Plan:8.2] Write deployment instructions and pre-deployment checks in `README.md`.
- [ ] T030 [Plan:8.2] Prepare a deployment plan with target, cost, secrets, and rollback details; do not deploy until the user explicitly approves.

## Requirement Mapping

| REQ ID | Description | Plan Items | Implementation Evidence |
|--------|-------------|------------|--------------------------|
| REQ-001 | Distributed workers, outbox/idempotency, bounded retry/DLQ, leases, stale-worker protection | 4.1, 4.2 | `JobExecutionService.java`, `OutboxDispatchService.java`, `KafkaJobEventListener.java`, `KafkaWorkerConfig.java`, integration tests |
| REQ-002 | Redis rate limiting, backpressure, recovery, health, metrics, structured logs, dependency and concurrency tests | 5.1, 5.2 | Rate limiter, API exception handler, actuator configuration, worker/outbox services, operational tests |
| REQ-003 | Structured job proposals, RAG troubleshooting, evidence-grounded diagnosis, safe read-only tools, provider failure handling, evaluations | 6.1, 6.2 | `src/main/java/com/devflowai/ai/`, AI service tests and evaluation fixtures |
| REQ-004 | Full regression/integration/concurrency verification, performance benchmarks, factual results and limitations | 7.1, 7.2 | Maven/integration test results, benchmark sources, `README.md`, `docs/` |
| REQ-005 | Docker/Compose, CI, local startup/shutdown, deployment instructions, approval-gated deployment | 8.1, 8.2 | `Dockerfile`, `compose.yaml`, `.github/workflows/`, `README.md`; no deployment before explicit approval |

## Verification record and known limitations

- `mvn dependency:tree "-Dincludes=org.testcontainers"` resolves `testcontainers`, `testcontainers-junit-jupiter`, `testcontainers-mysql`, and `testcontainers-kafka` to 2.0.5. `mvn dependency:tree "-Dincludes=com.github.docker-java"` resolves Docker Java API and transports to 3.7.1.
- Testcontainers 1.21.0 previously resolved docker-java 3.4.2 and failed against this Docker Engine with HTTP 400 and an empty server description. With the 2.0.5 BOM, Testcontainers connected to Docker Desktop 4.63.0 / Engine 29.2.1 / API 1.53 over its detected Windows named pipe and started its disposable Redis 7 container. The runtime result supports a Docker-client API compatibility mismatch as the cause; the old stack's exact failing request was not captured, so no narrower Docker API operation is asserted as the root cause.
- `mvn "-Dtest=RedisRateLimitStoreIntegrationTest,RedisReadinessIntegrationTest,RedisRateLimitStoreUnavailableTest,ProductionTenantSecurityTest,JobExecutionServiceTest,OutboxDispatchServiceTest" test` — final run: 18 tests passed, 0 failures, 0 errors, 0 skipped. The first run found the readiness endpoint unavailable (404) unless the rate-limit profile was active; moving probe enablement into the base configuration fixed it.
- `mvn "-Dtest=WorkerPropertiesTest,KafkaWorkerConfigTest,JobExecutionServiceTest,JobLeaseTest,MySqlJobExecutionIntegrationTest,KafkaWorkerIntegrationTest" test` — 21 tests passed, 0 failures, 0 errors, 0 skipped.
- Earlier `mvn test` before the MySQL claim fix — 65 tests passed, 0 failures, 0 errors, 0 skipped.
- Live Redis verification covers Lua atomic increments, TTL and expiry/reset, 40 concurrent increments, tenant-isolated limits, rate-limit rejection and `Retry-After`, readiness loss, and a locally isolated connection refusal. The Redis outage exception is verified; the `503` HTTP mapping is covered by the rate-limit interceptor/exception-handler tests.
- `mvn "-Dtest=MySqlJobExecutionIntegrationTest" test` — latest run: 4 passed, 0 failed, 0 errors, 0 skipped against MySQL 8.4. Flyway ran both project migrations, but warned its current version is tested only through MySQL 8.1; compatibility with 8.4 is therefore demonstrated by this run, not declared by Flyway.
- `mvn "-Dtest=KafkaWorkerIntegrationTest" test` — 2 passed, 0 failed, 0 errors, 0 skipped against Kafka 3.8.0. The tests cover actual broker duplicate delivery and retry-to-DLT for an invalid persisted-job reference.
- `mvn "-Dtest=OutboxKafkaMySqlIntegrationTest,MySqlJobExecutionIntegrationTest,KafkaWorkerIntegrationTest,JobExecutionServiceTest,JobLeaseTest,OutboxDispatchServiceTest" test` — latest focused run: 26 passed, 0 failed, 0 errors, 0 skipped.
- `mvn "-Dtest=WorkerProcessRecoveryIntegrationTest#twoInstancesSustainConcurrentSubmissionsClaimsRetriesAndDuplicateKafkaEvents" test` — 1 passed, 0 failed, 0 errors, 0 skipped; `mvn "-Dtest=WorkerProcessRecoveryIntegrationTest" test` — 4 passed, 0 failed, 0 errors, 0 skipped against disposable MySQL 8.4/Kafka 3.8.0. A child worker JVM was killed after claiming a job and blocking in its handler; after its DB lease was manually expired, a replacement Spring context recovered the retry and the old lease token was rejected. A separate child dispatcher JVM was killed after broker acknowledgement but before marking the outbox row delivered; a replacement dispatcher reclaimed and republished the event. This proves the tested persisted state and duplicate-delivery handling, not external effect idempotency.
- The same class's two-context workload exercised 48 jobs for 12 tenants: 47 concurrent submissions across 8 submission threads, followed by one isolated retry; it sent 24 duplicate broker records and verified consumer-group catch-up. The latest full-suite run measured 5.471 seconds. The retry flow verifies that concurrent scheduling creates one retry outbox row in addition to the original event. Because global claim batches can also claim due retries when triggered by an unrelated Kafka event, the test drains the consumer group before isolating the retry-scheduler path. This is short integration contention, not production-scale or long-duration stress, and does not establish maximum retry exhaustion or broad fault recovery.
- Delivery is at-least-once, not exactly-once. The duplicate publication scenario verifies a persisted job executes once under the tested conditions; external handler side effects can still repeat after crash windows and must be idempotent.
- Flyway resolves as 11.7.2 (`flyway-core` and `flyway-mysql`). Its runtime warning says MySQL 8.4 is newer than its tested support ceiling, MySQL 8.1. Both migrations execute successfully on disposable MySQL 8.4; this is observed behavior, not a vendor support guarantee. No Flyway version was changed.
- An initial MySQL distinct-job concurrency run left one candidate `QUEUED`. Direct checks confirmed both jobs were present and the configured scheduler could select both; inspection found `JobEntity.claim` also rejecting `QUEUED` jobs whenever their persisted `scheduledAt` appeared ahead of the worker clock. Time gating now applies only to `RETRY_SCHEDULED`, and unit plus real MySQL concurrency tests pass. The integration handler is explicitly ordered ahead of the catch-all fallback. The first Kafka run reused one tenant quota for two separately seeded jobs; the marker job now uses a unique tenant.
- Final retry-race verification on 2026-10-13: `mvn "-Dtest=WorkerProcessRecoveryIntegrationTest#unrelatedKafkaEventCannotClaimDueRetryBeforeTransactionalOutboxScheduling" test` — 1 passed, 0 failed, 0 errors, 0 skipped; `mvn "-Dtest=WorkerProcessRecoveryIntegrationTest" test` — 5 passed, 0 failed, 0 errors, 0 skipped; latest `mvn test` — 78 passed, 0 failed, 0 errors, 0 skipped. No tests were skipped in the latest full suite. Initial test attempts uncovered outbox event timestamp-tie selection and a consumer-group catch-up check that ran before confirming retry-event delivery; both test synchronization/selection issues were corrected before the passing final runs.
- Root cause and fix: the global worker claim query previously accepted both `QUEUED` and `RETRY_SCHEDULED`, letting an unrelated Kafka event claim a due retry before its poller transaction had created the corresponding retry outbox event. Claim eligibility is now restricted to `QUEUED`. The new regression verifies the retry stays scheduled across unrelated event consumption, races two independent schedulers and consumers, verifies persisted state/outbox consistency, exercises duplicate delivery and consumer-group catch-up, and confirms terminal retry exhaustion. Retry behavior and at-least-once delivery are preserved; no exactly-once or external-effect guarantee is claimed.
- Worker metrics now include processing latency with finite outcome tags, queue-age timing, claim-batch size/duration, claimed count, active-worker gauge, and existing retry/recovery counters. Claim batch size and Kafka listener concurrency are independent configuration values. Scheduler-wide admission limits still bound actual selected work.
- Latest `mvn "-Dtest=JobProcessingPerformanceBaselineIT" test` — 1 passed, 0 failed, 0 errors, 0 skipped. Java 21.0.10/MySQL 8.4, 1,200 running jobs, 250 queued jobs, 60 submissions, 2 warm-up batches, and 8 measured worker batches. Same-run old entity query vs grouped aggregate, 12 repetitions each: 14,400 vs 0 entity loads; p50/p95 37/70 ms vs 8/15 ms. Candidate query p50/p95 5/6 ms; submission throughput 30 ops/s, p50/p95 43/58 ms; worker throughput 40 jobs/s, batch p50/p95 658/713 ms, 1,540 statements and 1,120 entity loads. Grouped query `EXPLAIN` reported `Using temporary`; candidate selection used `idx_jobs_dispatch` and filesort; the small fixture outbox query scanned 60 rows. No index was added based on this plan alone. Timings vary and no generalized improvement percentage is claimed. Full measurements and caveats are in `TESTING.md`.
- Production identity-provider integration is absent; protected API access remains fail-closed unless a trusted principal is supplied.
- Spring AI major/version compatibility with Spring Boot 3.5 must be resolved before adding its dependencies.
- Provider keys and production identity credentials are not required for local development; use environment-variable placeholders and test doubles.
- Deployment is explicitly approval-gated and will not be run as part of implementation without approval of target and potential costs.
- A first run of the new worker test failed because its directly seeded jobs omitted the scheduler-required `scheduledAt`; the fixture was corrected and subsequent focused and full suites passed.
