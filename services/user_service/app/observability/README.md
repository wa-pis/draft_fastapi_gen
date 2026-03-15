### Observability

This folder contains logging, metrics, and tracing setup for the service.

- Structured JSON logging with correlation identifiers.
- Prometheus metrics and the `/metrics` endpoint.
- OpenTelemetry tracing configuration and exporters.

All services should rely on shared patterns here so logs, metrics, and traces are consistent across the platform.

