### Infrastructure Layer

This folder contains adapters to external systems and frameworks.

- Database access (SQLAlchemy sessions, repositories, migrations glue).
- Kafka producer/consumer setup and outbox integration.
- Redis client and other caches or coordination tools.
- Integrations with external APIs, model registries, or feature services.

Code here depends on external libraries but should expose interfaces that the `application` layer can use.

