# Architecture

## Current scope

DevFlow AI is a Java 21 / Spring Boot 3.5.0 modular monolith with a profile-gated Kafka worker runtime. Job submission persists the job, history, and dispatch outbox record in one database transaction. The default profile does not start Kafka polling or worker recovery loops.

## Submission and scheduling boundary

1. The API validates the request and obtains tenant identity from the security context.
2. A transaction locks or creates the tenant quota row, checks active job count and aggregate active resource units, then stores the job, history, and a versioned `JobEvent` outbox row.
3. With the `kafka` profile enabled, a poller claims due outbox rows using a database lease, publishes keyed JSON events to Kafka, and records delivery only after broker acknowledgement.
4. A Kafka consumer validates event type/version/tenant/correlation against the persisted job. It asks the weighted-priority scheduler to select eligible persisted candidates and atomically claims selected jobs under quota-row and job-row locks before invoking a registered handler.

The outbox closes the database-commit-to-publish gap, but delivery is **at least once**, not exactly once. A crash after Kafka acknowledgement and before the `DELIVERED` update can publish the same stable event ID again. Consumers rely on persisted state and lease claims to avoid re-claiming completed/running jobs. Retry dispatch uses a new event ID and the original correlation ID.

Each Kafka event causes at most one scheduler claim batch. The batch is bounded by scheduler admission limits, so a single record cannot drain an arbitrarily large backlog while holding the consumer thread. Remaining jobs are admitted by subsequent events.

## Persistence and concurrency

- `jobs`, `tenant_quotas`, and `job_execution_history` are defined by Flyway V1; V2 adds correlation/result/lease columns and `outbox_events`. The project resolves Flyway 11.7.2. Its runtime warns that MySQL 8.4 is beyond its tested ceiling of 8.1; both migrations have executed successfully against disposable MySQL 8.4 in this project, which is observed project behavior rather than an official support guarantee. See the version-pinned [Flyway 11.7.2 MySQL compatibility source](https://github.com/flyway/flyway/blob/flyway-11.7.2/flyway-database/flyway-mysql/src/main/java/org/flywaydb/database/mysql/MySQLDatabase.java).
- Submission serializes quota checks per tenant using a pessimistic write lock on the tenant quota row. A missing quota row is created in the transaction; unique-key races are retried after rollback.
- Active job count and active resource-unit usage include `ACCEPTED`, `QUEUED`, `RUNNING`, and `RETRY_SCHEDULED`.
- The `(tenant_id, idempotency_key)` unique constraint is the final duplicate guard. After a constraint failure, submission returns the already-persisted same-tenant job when it exists.
- H2 tests cover outbox persistence and publisher acknowledgement/failure/reclaim, duplicate worker event handling, execution retry/dead-letter, worker duplicate claims, and scheduling order/age promotion. Disposable MySQL 8.4/Kafka 3.8.0 integration tests cover transaction rollback, persisted outbox publication/acknowledgement, broker pause/recovery, duplicate publication and reclaim, and contention across two independent Spring contexts. A separate worker JVM is killed after claiming work and blocking in its handler; the lease is then marked expired in MySQL and a replacement context recovers the retry while the stale lease token is rejected. A separate dispatcher JVM is killed after Kafka acknowledgement but before the outbox-delivery update; a replacement dispatcher reclaims and republishes the event. Expiration is forced through targeted test SQL rather than waiting for elapsed wall-clock time. These tests establish only their tested state transitions and do not establish exactly-once external effects or general multi-instance correctness.

## Tenant identity and security

- The `development` Spring profile enables `X-Tenant-Id` and the `dev-tenant` fallback for local development only.
- Outside `development`, requests require authentication. An authenticated principal must implement `AuthenticatedTenantPrincipal` and provide a nonblank tenant ID; otherwise the request is denied.
- No production authentication provider or token-to-principal adapter is configured. Production API access is therefore intentionally fail-closed until one is supplied. A client-controlled header is not production authentication.
- Tenant identity is not accepted as a job request-body field or listing query parameter. Persistence lookups and list queries use the resolved tenant.

## Lifecycle and quotas

Job status changes go through `JobEntity.transitionTo`, which checks the centralized transition graph. `QUEUED` means immediately dispatchable; the scheduled-time guard applies only to `RETRY_SCHEDULED`, rather than allowing the retry timestamp to block a ready queued job. A worker claims a queued job into `RUNNING` with an expiring random lease token and increments attempts at claim time. Success, failure, retry scheduling, dead-lettering, and execution history/result writes are persisted. The worker rejects completion/failure after lease expiration; recovery can move an expired execution to retry or dead-letter. Lease tokens fence stale database writes, but cannot stop a stale handler from repeating external side effects: handlers that call external systems must be idempotent and honor the execution context's fencing token.

Tenant quotas cover active-job count and aggregate active resource units. Worker claims lock all quota rows before calculating running work and claiming candidates; this serializes claims in databases honoring the configured pessimistic locks. The latest two-context integration workload submitted 47 normal jobs across 12 tenants with eight submitter threads, then isolated one retry job, sent 24 duplicate broker events, and verified consumer-group catch-up. It completed in 5.471 seconds during the full suite. Assertions verified attempt counts, tenant mismatch rejection, retry outbox persistence, and no remaining active/retry states. This short workload is not a production stress test. H2 tests do not establish production database locking behavior. The scheduler's aging promotion (one priority level per 300 seconds, capped at critical) helps prevent starvation, while tenant and resource caps constrain selection. The seeded performance benchmark is not a long-running fairness or starvation test.

## API and validation

`/api/v1/jobs` supports submission, tenant-scoped lookup/listing/history, cancellation, and retry. Listing filters status and priority, and accepts zero-based pagination with page sizes from 1 through 100. Validation and domain failures map to 400, 404, or 409 responses. OpenAPI annotations describe the endpoints and expected status codes.

## Operations and remaining verification

- Enable the `kafka` profile and configure `DEVFLOW_KAFKA_BOOTSTRAP_SERVERS` to run outbox polling, worker consumption, retry recovery, lease renewal, and Kafka dead-letter publishing. The configured DLT topic is `devflow.job-events.v1.DLT` by default.
- Outbox retries are exponential with jitter, capped at 300 seconds and 12 publish attempts; exhausted outbox records enter the database `DEAD_LETTER` state. Kafka listener failures use Spring Kafka exponential retry before publishing to the Kafka DLT.
- Actuator metrics include pending/dead-letter outbox counts and running/dead-letter job counts, along with publish/worker counters. Worker metrics also include processing duration by finite outcome, queue age, claim batch size and duration, claimed-job count, and active worker count. Metric labels exclude job, tenant, correlation, and event IDs.
- Worker claim capacity (`devflow.worker.claim-batch-size`, environment override `DEVFLOW_WORKER_CLAIM_BATCH_SIZE`) and Kafka listener concurrency (`devflow.worker.concurrency`, override `DEVFLOW_WORKER_CONCURRENCY`) are independently configurable and must each be at least 1. Scheduler-wide admission limits remain a separate cap on selected work.
- Liveness/readiness probes are enabled in the base configuration. Redis is included in readiness only when the `rate-limit` profile is active; the production readiness route is unauthenticated for platform probes while job API routes remain protected.
- The optional rate limiter uses a Redis Lua fixed-window counter. Redis availability, counter expiry, concurrent increments, tenant isolation, and readiness-down behavior have been exercised against a disposable Redis 7 container; a separate isolated refused local connection verifies fail-closed behavior. This does not establish Redis failover/recovery characteristics.
- Local H2 tests use Hibernate schema generation and explicitly do not execute Flyway. Flyway resolves as 11.7.2; it warns that MySQL 8.4 is newer than its tested support ceiling of MySQL 8.1. Both migrations ran successfully in the disposable MySQL 8.4 tests, but that is not a vendor support guarantee.
- Kafka producer/consumer behavior, duplicate delivery, retries, and DLT routing have been verified with a disposable Kafka broker. Separate JVM kill tests verify worker recovery after forced lease expiry and outbox reclaim after post-acknowledgement dispatcher termination. Outbox delivery is at-least-once, not exactly-once, and no external handler side effect is exercised by the duplicate-delivery test.
- Worker claim selection is restricted to `QUEUED` jobs. A due `RETRY_SCHEDULED` job becomes claimable only after `scheduleDueRetries` transactionally transitions it to `QUEUED` and inserts the retry outbox event. This prevents an unrelated Kafka event from bypassing retry scheduling and claiming a retry before its event is persisted. The regression test exercises the unrelated-event race, competing application contexts, matching retry state/outbox persistence, duplicate delivery and consumer-group catch-up, plus retry exhaustion. Its final verified run passed as part of the five-test recovery class and 78-test full suite; it does not establish production-scale behavior or exactly-once external effects.
- Operational metrics and worker capacity configuration are implemented as described above. Redis failover/network partition behavior, prolonged Kafka/MySQL outage recovery, natural elapsed-time lease expiration under process supervision, production-scale database behavior, retry exhaustion in the two-instance workload, and long-running scheduler fairness remain unverified. A repeatable seeded MySQL performance baseline and its exact measurements are recorded in `TESTING.md`; those environment-specific timings are not a production guarantee.
- Production authentication remains unconfigured. The development `X-Tenant-Id` mechanism is not production authentication; production remains fail-closed until a trusted provider supplies `AuthenticatedTenantPrincipal`.
- No exactly-once execution guarantee is made. Outbox publication is at-least-once; handler side effects may repeat after crashes.
