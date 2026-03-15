### Service Application Layout

This directory contains the runtime code for the `user_service` following clean architecture and DDD principles.

- `api/` – FastAPI routers and Pydantic schemas (pure HTTP layer).
- `application/` – use-cases, orchestration logic, idempotency utilities.
- `domain/` – domain entities and domain events.
- `infra/` – DB, Kafka, Redis, and outbox adapters.
- `observability/` – logging, metrics, and tracing configuration.
- `resilience/` – circuit breaker and retry helpers.
- `config/` – environment-driven settings (`Settings`).
- `workers/` – background tasks (Kafka consumers, outbox publisher, pipelines).
- `main.py` – FastAPI entrypoint that wires these layers together.

All new functionality should be added in the appropriate layer to keep concerns separated.
