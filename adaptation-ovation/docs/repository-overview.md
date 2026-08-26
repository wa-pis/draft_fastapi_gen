# Repository overview

## Purpose

This repository contains a Python 3.12 service that moves long-running calculations out of the
Kafka consumer loop. Kafka remains the public event transport, while PostgreSQL and DBOS provide a
durable internal workflow queue. The result is one distributable binary with two independently
scalable process roles:

- `calculation-worker ingress` consumes, validates, and durably accepts work;
- `calculation-worker worker` executes accepted work and publishes its lifecycle events.

The service is intentionally not a general-purpose job platform. It has no HTTP status API, user
interface, scheduler, dead-letter queue, automatic retry policy, or global concurrency controller.

The component and deployment view is in [`architecture.puml`](architecture.puml).

## End-to-end behavior

1. A producer writes `calculation.requested` to the shared Kafka `INTEGRATIONS` topic.
2. An ingress replica decodes JSON, validates the version 1 event, and routes it to the registered
   request handler.
3. A valid request becomes a DBOS workflow in PostgreSQL. Its deterministic workflow ID is UUIDv5
   over `(request_id, calc_process)`, so redelivery does not create a second logical job.
4. Only after the enqueue has succeeded does ingress synchronously commit the Kafka offset.
5. A worker claims the workflow, publishes `calculation.started`, fetches input from the upstream
   HTTP API, runs the selected calculation handler, and publishes `calculation.completed`.
6. A handled validation, upstream, unsupported-process, or calculation failure produces
   `calculation.failed` when enough request identity is available. Every failure event is terminal
   and has `retryable=false`.

All lifecycle events use deterministic IDs and the original `request_id` as their Kafka key.
Consumers should deduplicate by `event_id` because PostgreSQL checkpoints and Kafka acknowledgements
cannot be committed in one atomic transaction.

## Runtime roles

### Ingress

Ingress owns the Kafka consumer, a Kafka producer for validation failures, and a lightweight DBOS
client. It processes one polled record at a time with Kafka auto-commit and auto-offset-store both
disabled. Unknown but structurally valid event types are ignored and committed because the topic is
shared with other integrations.

Malformed UTF-8, invalid JSON, and unidentifiable invalid envelopes are logged, counted, discarded,
and committed. There is no DLQ. An invalid `calculation.requested` with usable `request_id`,
`calc_id`, and `calc_process` publishes `calculation.failed` before the offset is committed.

### Worker

Worker launches the DBOS runtime and a configured calculation workflow. Each replica runs at most
`DBOS_WORKER_CONCURRENCY` workflows at once; the default is four. Capacity scales per replica, so
the repository does not impose a cluster-wide limit.

The workflow steps are deliberately small and checkpointed:

1. `calculation.publish_started`
2. `calculation.fetch_input`
3. `calculation.calculate`
4. `calculation.publish_completed`

The error path invokes `calculation.publish_failed`. Every step sets `retries_allowed=False`, and
the upstream client makes one HTTP attempt. A normal exception therefore ends the workflow in
`ERROR`; it is not automatically run again.

Crash recovery is a separate mechanism. A replacement process with the same `DBOS_EXECUTOR_ID` and
`DBOS_APPLICATION_VERSION` can resume a workflow interrupted by process loss. DBOS skips completed
steps and may execute only the uncheckpointed step again. This leaves a narrow window in which an
external side effect was acknowledged but its checkpoint was not persisted, so downstream event
deduplication is still required.

## Source map

| Path | Responsibility |
| --- | --- |
| `src/calculation_worker/main.py` | CLI parsing, settings loading, signal handling, process lifecycle, and exit codes |
| `src/calculation_worker/bootstrap.py` | Composition root that builds ingress or worker dependencies and closes resources |
| `src/calculation_worker/domain/` | Pydantic event contracts and immutable message/job values |
| `src/calculation_worker/application/` | Message routing, ingress handling, consumer orchestration, event creation, ports, registries, and the DBOS workflow |
| `src/calculation_worker/calculations/` | Calculation extension contract, handler registry, and the current example implementation |
| `src/calculation_worker/infrastructure/dbos_queue.py` | Durable enqueue adapter and deterministic workflow identity |
| `src/calculation_worker/infrastructure/kafka.py` | Synchronous Kafka consumer/producer adapters and delivery acknowledgement handling |
| `src/calculation_worker/infrastructure/upstream.py` | One-attempt authenticated HTTP client and error translation |
| `src/calculation_worker/infrastructure/observability.py` | Structured JSON logs and per-process Prometheus metrics server |
| `src/calculation_worker/settings.py` | Environment-based configuration and cross-field validation |
| `tests/unit/` | Fast tests for domain, application, configuration, and adapter behavior |
| `tests/integration/` | Opt-in Docker/PostgreSQL verification of DBOS durability and deduplication |
| `Dockerfile` | Reproducible migration and non-root runtime images |
| `Makefile` | PyInstaller one-file binary build |
| `pyproject.toml` / `uv.lock` | Python 3.12 dependency, tooling, and lock-file definition |
| `.env.example` | Deployable configuration reference without real secrets |

The source follows a ports-and-adapters shape: application code depends on small protocols, while
Kafka, DBOS, HTTP, and metrics live behind infrastructure implementations. `bootstrap.py` is the
only place that assembles concrete runtime dependencies.

## Calculation extension point

A calculation handler exposes a unique `process_name` and implements two operations:

```python
def fetch_input(request: CalculationRequested) -> Mapping[str, Any]: ...


def calculate(
    request: CalculationRequested,
    input_data: Mapping[str, Any],
) -> CalculationResult: ...
```

The current `example` handler fetches one upstream JSON object and returns whether data exists plus
the number of top-level fields. It is a working integration example, not domain-specific business
logic. New implementations belong in `calculations/` and must be registered in
`build_worker_runtime`. Duplicate process names fail at startup; unknown names fail the workflow
with `UNSUPPORTED_CALCULATION`.

Workflow names and configured-instance names form part of persisted DBOS history. Incompatible
workflow changes therefore require a new `DBOS_APPLICATION_VERSION`, with the old version kept
running until its work has drained.

## Configuration, operations, and observability

Configuration comes from environment variables and an optional local `.env`. PostgreSQL is required
in both roles. A stable executor ID and upstream URL/token are additionally required in worker mode.
Kafka supports plaintext, SSL, and SASL settings; secrets are excluded from settings
representations and validation errors.

Schema migration is an explicit deployment step. Runtime starts DBOS with `run_migrations=False`
and must use an already migrated schema. The Dockerfile provides:

- a `migrations` target whose entry point is `dbos migrate`;
- a `runtime` target containing only the PyInstaller binary and running as a non-root user.

Both runtime roles emit structured JSON logs to stdout and expose process-local Prometheus metrics
on `METRICS_PORT`. Metrics cover Kafka intake/publication, handler latency, calculation results and
duration, upstream requests, and the last successfully committed input.

SIGTERM and SIGINT request graceful shutdown. Ingress stops polling and closes Kafka/DBOS resources.
Worker gives active DBOS workflows up to `DBOS_SHUTDOWN_GRACE_SECONDS` before shutdown; unfinished
work remains durable for crash recovery.

## Build and verification

The project uses `uv` with a committed lock file. The standard verification sequence is:

```bash
uv sync --python 3.12 --group build
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy src
make build
./dist/calculation-worker --help
```

Normal `pytest` excludes the integration marker. `uv run pytest -m integration` requires Docker and
PostgreSQL. The one-file executable must be built on the same operating system and architecture on
which it will run.
