### AI Platform Monorepo

This repository contains a distributed AI platform built around FastAPI, Kafka, PostgreSQL, Redis, and a full observability stack.

- **Clean architecture & DDD**: each service is split into `api`, `application`, `domain`, and `infra` layers.
- **Event-driven**: services publish domain events via the outbox pattern into Kafka.
- **Resilience & observability**: circuit breakers, retries, metrics, tracing, and structured logging are provided as shared patterns.

See the `services` directory for individual bounded-context services and `scripts` for automation utilities (e.g. scaffolding).

Standalone applications:

- [Async PostgreSQL Writer](async-postgres-writer/README.md): FastAPI server running on Uvicorn. It streams NDJSON into PostgreSQL with atomic COPY and serves committed results as pages or NDJSON.
- [Async PostgreSQL Client](async-postgres-client/README.md): separately installable async Python client for the FastAPI server.

Start the FastAPI server and PostgreSQL 17 locally with Docker Compose (demo credentials):

```bash
cd async-postgres-writer
docker compose --env-file .env.example build writer
docker compose --env-file .env.example up --no-build -d writer
```

Open [Swagger UI](http://127.0.0.1:8080/docs) or see the
[OpenAPI 3.1 contract](async-postgres-writer/openapi/stream-writer.v1.yaml).
