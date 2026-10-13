# Design Decisions

## Java 21 and Spring Boot 3.5.0

Java 21 provides the latest long-term support baseline for modern backend work. Spring Boot 3.5.0 is a compatible, production-ready baseline for a new backend project and matches the requirement for a secure, maintainable foundation.

## Modular monolith first

The project is intentionally designed as a modular monolith rather than a distributed system from day one. This keeps the initial build simple while preserving the ability to add asynchronous execution and external integrations later.

## Tenant-aware scheduling

Job ordering is based on priority and fairness across tenants rather than simple FIFO processing. This is a core requirement for multi-tenant workloads and prevents one tenant from monopolizing the system.

## Profile-gated Kafka runtime

Kafka dependencies and worker components are enabled only for the `kafka` profile. This keeps compile and local H2 tests independent of a running broker while making real dispatch opt-in. Redis and AI integrations are not part of this phase.

## Tenant identity profiles

`X-Tenant-Id` is accepted only with the explicit `development` profile. Other profiles require authenticated requests and a principal implementing `AuthenticatedTenantPrincipal`; no production identity provider is included. This keeps tenant resolution behind a trusted-principal contract without treating a client header as authentication.

## Per-tenant quota serialization

Submissions lock the tenant's quota row while calculating active-job and active-resource totals. First-use quota creation is protected by its unique tenant key; an integrity failure rolls back and retries, allowing the competing transaction to create or reuse the row. The corresponding concurrency tests use H2; production MySQL behavior still requires integration verification.

## Transactional outbox and delivery semantics

The submission transaction writes the job, history, and outbox event together. A leased poller publishes outside the claim transaction and marks delivery only after Kafka acknowledgement. A process crash after acknowledgement and before marking delivery can cause republishing, so delivery is at-least-once and consumers must be idempotent. Exactly-once execution is not claimed.

## Worker leases and fencing

Workers persist a `RUNNING` claim, increment attempts at claim time, and receive an expiring token. Completion/failure writes require the current unexpired token; expired leases are recovered into retry/dead-letter state. This fences database results, not side effects already performed by an old worker. Job handlers must use idempotency and honor the fencing token for external operations.

## Scheduler admission and fairness

The existing weighted-priority ordering is retained with age-based priority promotion every 300 seconds (capped at critical). Worker selection additionally respects global concurrent-job, per-tenant concurrent-job, and per-tenant aggregate resource-unit caps. Local tests check ordering, age promotion, and admission limits; production fairness and database concurrency still require load/integration validation.

## MySQL and Kafka verification

Flyway V1/V2 target MySQL. The environment does not have a Docker daemon, so migrations, MySQL locks, Kafka publishing/consumption, and DLT behavior have not been validated against real infrastructure. H2 schema generation and mock publisher tests are not substitutes for those checks.
