# Research Notes

## Scope

DevFlow AI is a backend-only job orchestration platform for multi-tenant, resource-aware execution. The project will focus on a modular monolith architecture that can evolve into a distributed deployment without forcing unnecessary complexity at Phase 1.

## Technology selection

- Java 21 is the baseline runtime because it provides strong language features, long-term support, and compatibility with current Spring Boot releases.
- Spring Boot 3.5.0 is selected for compatibility with Java 21 and the current Spring ecosystem.
- Maven is used for dependency management and repeatable builds.
- H2 is used only in tests so the project compiles and verifies without requiring live infrastructure credentials.

## Architectural principles

- Multi-tenant isolation is enforced at the scheduling boundary and in all job metadata.
- Jobs are resource-aware and tracked with explicit constraints rather than implicit assumptions.
- Scheduling is fair by tenant and priority, with starvation prevention and bounded concurrency.
- The platform favors at-least-once processing semantics and idempotent execution patterns where automation is asynchronous.
- External services remain optional at compile time and are introduced through configuration rather than hardcoded secrets.

## Phase 1 goals

- Establish the Java 21 / Spring Boot baseline.
- Define tenant-aware job models and scheduling rules.
- Validate constraints via tests.
- Prepare the project for later API, persistence, worker, and messaging layers.

## Constraints

- No live MySQL, Redis, Kafka, or Gemini credentials are required for initial compile/test validation.
- The system keeps a strict separation between runtime infrastructure and the application domain model in early phases.
- No frontend code is included in this repository.
