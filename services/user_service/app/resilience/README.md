### Resilience

This folder provides patterns for reliable interactions with external systems.

- Circuit breaker implementation to protect dependencies.
- Retry helpers with exponential backoff and jitter.
- Building blocks that can be composed around DB, Kafka, Redis, HTTP clients, or model-serving calls.

Application and infrastructure code should use these helpers instead of hand-rolled retries or ad-hoc error handling.

