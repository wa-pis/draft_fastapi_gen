### Background Workers

This folder contains long-running worker processes for the service.

- Outbox publishers that read pending events from the database and publish to Kafka.
- Kafka consumers for processing domain events and executing workflows.
- Pipelines for model inference, feature generation, or batch jobs.

Workers reuse the same `application`, `domain`, and `infra` layers as the HTTP service but have their own entrypoints and deployment configuration.

