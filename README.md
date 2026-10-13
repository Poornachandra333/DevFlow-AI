# DevFlow AI

DevFlow AI is a backend-only, multi-tenant job orchestration platform built on Java 21 and Spring Boot 3.5.0. Unit/H2 validation does not require live infrastructure; the complete suite includes Docker-backed Redis, MySQL, and Kafka integration tests.

## Current capabilities

- Job submission API with validation and idempotency protection
- Persistent job, execution-history, result, lease, and transactional outbox models
- Tenant active-job and aggregate active-resource quota enforcement
- Cancellation and bounded retry flows with explicit state transition rules and dead-letter handling
- Profile-gated Kafka outbox publisher and worker using versioned job events, persisted claims, leases, recovery, and scheduler admission
- Bounded work per Kafka delivery so one event cannot drain an unbounded backlog on the consumer thread
- Optional Redis-backed, tenant-scoped job-submission rate limiting (`rate-limit` profile; 60 submissions per 60-second fixed window by default)
- Bounded-cardinality worker metrics for processing latency/outcomes, queue age, claim batches, retries, and active execution
- Independently configurable worker claim batch size (`DEVFLOW_WORKER_CLAIM_BATCH_SIZE`) and Kafka listener concurrency (`DEVFLOW_WORKER_CONCURRENCY`)
- Logstash JSON console logs with job, correlation, and outbox event identifiers
- H2-backed unit tests plus disposable Redis, MySQL, and Kafka Testcontainers integration tests

Delivery is at-least-once, not exactly-once. A duplicate Kafka event is expected to be safe for persisted job claims; handlers that cause external side effects must independently be idempotent and honor the fencing token. Current real-service tests verify selected duplicate delivery, DLT, persistence, rollback, and concurrency scenarios; they do not prove general multi-instance correctness or the full outbox-to-broker recovery lifecycle.

## Project layout

- `docs/` – architecture, research, and implementation decisions
- `src/main/java/com/devflowai/api` – REST controllers and DTOs
- `src/main/java/com/devflowai/config` – Spring configuration and scheduling beans
- `src/main/java/com/devflowai/domain` – JPA entities
- `src/main/java/com/devflowai/job` – scheduling domain model
- `src/main/java/com/devflowai/repository` – persistence access
- `src/main/java/com/devflowai/service` – business logic and job lifecycle transitions
- `src/main/resources` – application defaults and Flyway migration scripts
- `src/test/java` – unit and integration tests

## API endpoints

- `POST /api/v1/jobs` – submit a job with tenant-scoped idempotency key
- `GET /api/v1/jobs/{jobId}` – fetch a single job by tenant
- `GET /api/v1/jobs` – paginated job listing with status/priority filters
- `GET /api/v1/jobs/{jobId}/history` – execution timeline for a job
- `POST /api/v1/jobs/{jobId}/cancel` – cancel eligible jobs
- `POST /api/v1/jobs/{jobId}/retry` – retry eligible failed jobs

## Data model

- `jobs` stores job identity, tenant ownership, priority, payload, retry state, and timestamps
- `job_execution_history` records all lifecycle transitions for auditability
- `tenant_quotas` enforces per-tenant concurrency and capacity limits
- `outbox_events` durably records dispatch events and retry/lease/publish state

## Running the Kafka worker profile

1. Start Kafka and create or allow auto-creation of `devflow.job-events.v1` and `devflow.job-events.v1.DLT`.
2. Set `DEVFLOW_KAFKA_BOOTSTRAP_SERVERS` to the broker address.
3. Configure the production MySQL connection and run Flyway V1 and V2 on a disposable/test database first.
4. Start the application with the `kafka` Spring profile. This enables the outbox poller, Kafka listener, retry/lease recovery, and lease heartbeat.
5. Verify Actuator metrics for outbox pending/dead-letter events and running/dead-letter jobs; inspect logs and Kafka consumer-group/DLT state during failure tests.

Integration tests use Testcontainers 2.0.5, which successfully connected to Docker Desktop 4.63.0 / Docker Engine 29.2.1 (API 1.53) over the detected Windows named pipe. MySQL migration and selected locking behaviors were exercised on disposable MySQL 8.4, and broker duplicate/DLT behavior was exercised on Kafka 3.8.0. Flyway warned that its current version is tested through MySQL 8.1 only. Do not rely on these bounded integration scenarios as proof of multi-instance locking compatibility.

## Local development

1. Run `mvn clean test` for the complete unit and integration suite (Docker is required for Testcontainers); use focused `-Dtest=...` selectors for narrower validation.
2. Run `mvn package` to produce the application artifact.
3. Run the application with the `development` Spring profile to enable the local `X-Tenant-Id` identity header. This is development-only.
4. Other profiles require authentication and a trusted principal implementing `AuthenticatedTenantPrincipal`. No production authentication provider is included, so protected API access remains fail-closed until one is configured.
5. To run actual dispatch, configure MySQL/Kafka and enable `kafka`; see the manual steps above.
6. To enable submission rate limiting, activate the `rate-limit` profile and configure `DEVFLOW_REDIS_URL`. The limiter hashes tenant IDs for Redis keys, returns `429` with `Retry-After` when the limit is exceeded, and fails closed with `503` if Redis is unavailable.

## Verification record

- 2026-10-13: `mvn dependency:tree "-Dincludes=org.testcontainers"` resolved `testcontainers` and `testcontainers-junit-jupiter` to **2.0.5**. Testcontainers 1.21.0 previously resolved docker-java 3.4.2 and failed with HTTP 400 / an empty Docker server description. Version 2.0.5 resolves docker-java 3.7.1 and successfully connected to Docker Engine 29.2.1/API 1.53. The observed success supports a Docker Java client/API compatibility mismatch as the cause; the exact old failing engine operation was not captured.
- 2026-10-13: Focused command `mvn "-Dtest=RedisRateLimitStoreIntegrationTest,RedisReadinessIntegrationTest,RedisRateLimitStoreUnavailableTest,ProductionTenantSecurityTest,JobExecutionServiceTest,OutboxDispatchServiceTest" test` — **18 passed, 0 failed, 0 errors, 0 skipped**.
- 2026-10-13: `mvn test` — **56 passed, 0 failed, 0 errors, 0 skipped**. Surefire reports confirm that all Redis Testcontainers tests executed.
- 2026-10-13: Earlier `mvn "-Dtest=MySqlJobExecutionIntegrationTest" test` — **3 passed, 0 failed, 0 errors, 0 skipped** against disposable MySQL 8.4.
- 2026-10-13: Latest `mvn "-Dtest=MySqlJobExecutionIntegrationTest" test` — **4 passed, 0 failed, 0 errors, 0 skipped** against disposable MySQL 8.4, including duplicate and distinct-job concurrency.
- 2026-10-13: `mvn "-Dtest=KafkaWorkerIntegrationTest" test` — **2 passed, 0 failed, 0 errors, 0 skipped** against disposable Kafka 3.8.0.
- 2026-10-13: `mvn "-Dtest=WorkerPropertiesTest,KafkaWorkerConfigTest,JobExecutionServiceTest,MySqlJobExecutionIntegrationTest,KafkaWorkerIntegrationTest" test` — **16 passed, 0 failed, 0 errors, 0 skipped**.
- 2026-10-13: `mvn "-Dtest=WorkerPropertiesTest,KafkaWorkerConfigTest,JobExecutionServiceTest,JobLeaseTest,MySqlJobExecutionIntegrationTest,KafkaWorkerIntegrationTest" test` — **21 passed, 0 failed, 0 errors, 0 skipped**.
- 2026-10-13: Earlier `mvn test` before MySQL claim fix — **65 passed, 0 failed, 0 errors, 0 skipped**.
- 2026-10-13: Latest `mvn test` — **68 passed, 0 failed, 0 errors, 0 skipped**. Redis, MySQL, and Kafka containers started and the Surefire reports show the integration tests executed.
- Redis 7 live-container checks verify atomic increments, fixed-window expiry/reset, 40 concurrent increments, tenant isolation and configured limits, `429` with `Retry-After`, and readiness changing from `UP` to `DOWN`/503 after the test-owned Redis container stops. A separate test uses a closed local port to verify fail-closed behavior without contacting external Redis. The API's `503` mapping is also covered by focused rate-limit unit tests.
- The first focused run found the readiness probe returned 404 in the production-only profile because probes were enabled only by `rate-limit`; probe enablement is now in base configuration, and the final focused/full suites pass.
- See [TESTING.md](./TESTING.md) for commands, counts, runtime evidence, and verification boundaries. No performance benchmark was run.

## Current limitations

- Development header identity is not production authentication.
- Outbox publication is at-least-once; no end-to-end exactly-once execution guarantee is made.
- MySQL 8.4 tests exercise migrations, persistence rollback, concurrent duplicate delivery, and claims for two distinct jobs in one application process. Multi-instance behavior and Flyway's support for MySQL 8.4 remain unverified.
- Kafka 3.8.0 tests exercise broker duplicate delivery, retry, and DLT routing. End-to-end outbox publish acknowledgement/reclaim and broker outage recovery remain unverified.
- Lease fencing protects persisted state but not external side effects from a stale handler; integrations must be idempotent.
- Production authentication is not configured. Redis rate limiting is opt-in and live-container verified, but Redis failover/recovery behavior is not. Scheduler/worker performance benchmarks remain outstanding. GenAI/RAG integrations are not included.
