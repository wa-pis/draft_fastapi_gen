### Application Layer

This folder contains the application services and orchestration logic.

- Implements use-cases that coordinate domain entities and infrastructure adapters.
- Defines transaction boundaries and integrates cross-cutting concerns (e.g. idempotency).
- Does not contain HTTP-specific or persistence-specific code.

Typical contents:

- `*Service` classes implementing use-cases.
- Saga orchestrators for distributed workflows.
- Utilities like the `IdempotencyService` that are application-level concerns.

