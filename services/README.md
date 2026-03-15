### Services

This directory contains bounded-context services for the AI platform (e.g. `user_service`, `order_service`, `inference_service`).

Each service follows the same high-level layout:

- `app/main.py` – FastAPI entrypoint and middleware.
- `app/api` – HTTP routers and request/response models.
- `app/application` – use-cases, orchestration, and cross-cutting concerns.
- `app/domain` – domain entities, value objects, and domain events.
- `app/infra` – database, messaging, cache, and external system adapters.
- `app/observability` – logging, metrics, tracing setup.
- `app/resilience` – circuit breaker, retry helpers, and other resilience primitives.
- `app/workers` – background workers (Kafka consumers, outbox publishers, pipelines).

Use `scripts/scaffold_service.py` to scaffold new services with this structure.
