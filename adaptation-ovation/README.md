# Adaptation Ovation

`adaptation-ovation` is a Python 3.12 calculation service with Kafka as its public transport and
PostgreSQL/DBOS as its durable internal queue. Long calculations do not block the Kafka consumer.
The same PyInstaller binary runs in one of two explicit modes:

```bash
calculation-worker ingress
calculation-worker worker
```

- `ingress` consumes the shared `INTEGRATIONS` topic, validates messages, durably enqueues valid
  calculation requests in PostgreSQL, and only then commits the Kafka offset.
- `worker` runs DBOS workflows from `calculation-queue`, fetches input, calculates, and publishes
  lifecycle events back to Kafka. Each replica executes at most
  `DBOS_WORKER_CONCURRENCY` workflows concurrently (default: 4).

There is deliberately no status HTTP API. `calculation.started`, `calculation.completed`, and
`calculation.failed` remain the public result contract.

## Processing model

```text
Kafka INTEGRATIONS
        |
        v
  ingress process
  validate -> DBOS enqueue -> Kafka commit
        |                         \
        |                          -> invalid: failed or discard -> commit
        v
PostgreSQL DBOS queue
        |
        v
  worker process (4 concurrent workflows per replica)
  started -> fetch input -> calculate -> completed
                              \
                               -> terminal error -> failed
```

Unknown but valid `event_type` values are ignored and committed, allowing unrelated events on the
shared topic. Invalid UTF-8/JSON/envelopes are logged, counted, discarded, and committed because the
deployment has no separate dead-letter topic. An invalid `calculation.requested` that still contains
`request_id`, `calc_id`, and `calc_process` emits `calculation.failed`; without those identifiers it
is discarded after logging.

Each durable job contains only the validated calculation request. There is no DLQ and no automatic
replay path.

### Durable identity and duplicates

The logical job key is `(request_id, calc_process)`. It is converted to a deterministic UUIDv5 and
used as the DBOS workflow ID. Re-enqueueing the same logical job returns the existing workflow; it
does not start a second calculation and does not replace the first payload. Workflow history is not
automatically deleted, so this deduplication remains effective until an operator explicitly applies
a retention policy.

Lifecycle `event_id` values are also deterministic UUIDv5 values. Downstream Kafka consumers must
deduplicate by `event_id`: no database transaction can atomically cover both a PostgreSQL workflow
checkpoint and an external Kafka acknowledgement.

## Failure and recovery policy

Normal failures are **never retried**:

- Every DBOS step declares `retries_allowed=False`.
- The upstream HTTP client makes exactly one request. Timeouts, network failures, HTTP 429/5xx,
  other non-2xx responses, invalid JSON, and calculation exceptions are terminal.
- A handled calculation failure publishes `calculation.failed`, then leaves the DBOS workflow in
  `ERROR`.
- A Kafka publication failure also leaves the workflow in `ERROR`; automatic publication retry is
  disabled. An operator may explicitly inspect and resume/fork a workflow.

Crash recovery is different from retrying a failed task. DBOS checkpoints each completed step. If
an executor process dies, a replacement with the same `DBOS_EXECUTOR_ID` and
`DBOS_APPLICATION_VERSION` recovers its pending workflows. Completed steps are skipped; only the
step that had not durably completed can execute again. For example, an acknowledged and
checkpointed `calculation.started` is not intentionally republished. There is still a narrow
ack-before-checkpoint window in which any external side effect can occur twice.

`DBOS_MAX_RECOVERY_ATTEMPTS` limits repeated process-crash recovery (default: 3). It is not a retry
count for exceptions. Without DBOS Conductor, stable executor identity is operationally required:
when `worker-a` disappears, a replacement must come up as `worker-a` to recover its pending work.

On SIGTERM/SIGINT the worker stops taking new work, allows active workflows up to
`DBOS_SHUTDOWN_GRACE_SECONDS` (default: 30), then exits. Any incomplete workflow is recovered by a
replacement with the same executor ID.

## Workflow and extension contract

The durable workflow steps are:

1. `calculation.publish_started`
2. `calculation.fetch_input`
3. `calculation.calculate`
4. `calculation.publish_completed`

The terminal error path adds `calculation.publish_failed`. A calculation implementation registers a
unique `process_name` and implements:

```python
def fetch_input(request: CalculationRequested) -> Mapping[str, Any]: ...


def calculate(
    request: CalculationRequested,
    input_data: Mapping[str, Any],
) -> CalculationResult: ...
```

Register implementations in `build_worker_runtime`. Workflow code and configured instances must
remain available under the same DBOS names while old workflow rows may still need recovery.
Deploy workflow-incompatible code under a new `DBOS_APPLICATION_VERSION` and keep the old version
running until its work drains.

## Configuration

Copy `.env.example` to `.env`. Important DBOS settings are:

| Variable | Required/default | Purpose |
| --- | --- | --- |
| `DBOS_SYSTEM_DATABASE_URL` | Required | PostgreSQL URL for DBOS system tables |
| `DBOS_EXECUTOR_ID` | Worker only, required | Stable replica identity used for crash recovery |
| `DBOS_APPLICATION_VERSION` | `v1` | Version that owns and executes enqueued workflows |
| `DBOS_SYSTEM_SCHEMA` | `dbos` | PostgreSQL schema containing DBOS tables |
| `DBOS_WORKER_CONCURRENCY` | `4` | Maximum concurrent workflows per executor |
| `DBOS_MAX_RECOVERY_ATTEMPTS` | `3` | Maximum process-crash recoveries, not error retries |
| `DBOS_SHUTDOWN_GRACE_SECONDS` | `30` | Grace period for active workflows during shutdown |

`UPSTREAM_API_BASE_URL` and `UPSTREAM_API_TOKEN` are required only in worker mode. Kafka SASL/SSL,
timeouts, topic names, and metrics options are documented with defaults in `.env.example`.
`KAFKA_CLIENT_ID` should be unique per ingress replica; every ingress replica shares
`KAFKA_GROUP_ID`.

The database URL and API/Kafka credentials use secret settings and are excluded from settings
representations. Use a migration role for schema changes and a restricted runtime role for ingress
and workers.

## Migrations and containers

Migrations are explicit. Runtime processes configure DBOS with `run_migrations=False` and fail fast
if the schema is absent or stale.

Local migration command:

```bash
uv run dbos migrate \
  --sys-db-url "$DBOS_SYSTEM_DATABASE_URL" \
  --schema "$DBOS_SYSTEM_SCHEMA" \
  --app-role calculation_worker
```

The Dockerfile exposes separate targets:

```bash
docker build --target migrations -t adaptation-ovation-migrations .
docker run --rm adaptation-ovation-migrations \
  --sys-db-url 'postgresql://migration:secret@postgres/calculations' \
  --schema dbos \
  --app-role calculation_worker

docker build --target runtime -t adaptation-ovation .
docker run --env-file .env adaptation-ovation /app/calculation-worker ingress
docker run --env-file .env adaptation-ovation /app/calculation-worker worker
```

Run ingress and workers as separate deployments so they can scale independently. Multiple workers
have no global concurrency cap: total capacity is `4 × worker replicas` by default.

## Development and standalone build

```bash
uv sync --python 3.12 --group build
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy src
```

Docker-backed integration tests are opt-in:

```bash
uv run pytest -m integration
```

Build and smoke-test the one-file executable on the same OS/architecture where it will run:

```bash
make build
./dist/calculation-worker --help
```

PyInstaller explicitly collects DBOS, SQLAlchemy, and psycopg runtime modules. The binary still
requires network access to PostgreSQL, Kafka, and the upstream API; it does not embed database
credentials or configuration.
