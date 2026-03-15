### AI Platform Monorepo

This repository contains a distributed AI platform built around FastAPI, Kafka, PostgreSQL, Redis, and a full observability stack.

- **Clean architecture & DDD**: each service is split into `api`, `application`, `domain`, and `infra` layers.
- **Event-driven**: services publish domain events via the outbox pattern into Kafka.
- **Resilience & observability**: circuit breakers, retries, metrics, tracing, and structured logging are provided as shared patterns.

See the `services` directory for individual bounded-context services and `scripts` for automation utilities (e.g. scaffolding).
