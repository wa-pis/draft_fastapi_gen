### User Service

The `user_service` is a sample bounded-context service demonstrating the platform architecture.

- **Purpose**: manage user entities and emit user-related domain events.
- **Tech stack**: FastAPI, PostgreSQL, Kafka, Redis, OpenTelemetry, Prometheus.

Key components:

- `app/main.py` – FastAPI application, correlation middleware, router wiring.
- `app/api` – HTTP endpoints (`/health`, `/ready`, `/metrics`, `/v1/users`, etc.).
- `app/application` – application services (e.g. `UserService`), API idempotency.
- `app/domain` – `User` aggregate and domain events.
- `app/infra` – DB sessions, Kafka producer/consumer, Redis client, outbox repository.
- `app/observability` – JSON logging, tracing setup, metrics.
- `app/resilience` – circuit breaker and retry helpers for external calls.
- `app/workers` – background workers such as the outbox publisher.

Use this service as a template when creating new bounded contexts with the scaffolding script.
