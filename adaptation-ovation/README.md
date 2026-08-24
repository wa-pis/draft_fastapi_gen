# Adaptation Ovation

`adaptation-ovation` is a synchronous Python 3.13 Kafka worker. It consumes events from the shared
`INTEGRATIONS` topic, routes them by `event_type`, fetches calculation input from an upstream HTTP
API, runs the selected calculation, and publishes the result back to `INTEGRATIONS`. Terminally
failed inputs are also copied to `INTEGRATIONS.DLQ`.

The worker deliberately processes one record at a time. Parallelism comes from running more
instances in the same Kafka consumer group, not from `asyncio`, a thread pool, or manually assigning
partitions.

## Processing and architecture

The hot path is continuous:

```text
poll
  -> decode UTF-8 and JSON object
  -> validate the minimal event envelope
  -> route by MessageHandlerRegistry
  -> route calculation.requested by CalculationRegistry
  -> fetch upstream data and calculate
  -> publish every outgoing record
  -> wait for every delivery acknowledgement
  -> synchronously commit the input offset
  -> next poll
```

Unknown, valid `event_type` values are intentionally ignored and committed. This makes the shared
topic forward compatible. Malformed UTF-8/JSON, a JSON value that is not an object, or an invalid
minimal envelope is published to the DLQ before its input offset is committed.

```text
src/calculation_worker/
  main.py                         signals and process exit status
  bootstrap.py                    explicit dependency composition
  settings.py                     validated environment configuration
  application/
    consumer_loop.py              poll -> process -> publish -> commit
    message_processor.py          envelope validation and event routing
    registries.py                 MessageHandlerRegistry
    calculation_requested_handler.py
    event_factory.py              completed, failed, and DLQ records
  calculations/
    base.py                       CalculationHandler protocol
    registry.py                   CalculationRegistry
    example.py                    example calculation
  domain/models.py                input/output contracts
  infrastructure/
    kafka.py                      confluent-kafka adapters
    upstream.py                   long-lived httpx client and retries
    observability.py              JSON logging and Prometheus metrics
```

`bootstrap.py` creates settings, Kafka consumer and producer adapters, one long-lived
`httpx.Client`, both registries, handlers, processor, loop, metrics, and logging. Registration is
explicit: there are no mutable global registries, dynamic imports, or plugin framework.

### The two registries

| Registry | Key | Responsibility | Unknown key |
| --- | --- | --- | --- |
| `MessageHandlerRegistry` | `handler.event_type` | Routes a Kafka event to a message handler | Returns `None`; the event is ignored and committed |
| `CalculationRegistry` | `handler.process_name` | Selects calculation logic inside `calculation.requested` | Raises `UnsupportedCalculationError`, producing failed + DLQ records |

Both reject duplicate registration. The consumer loop only understands `HandlerResult`, so adding a
route never changes its publish/commit logic.

To add a Kafka event type:

1. Implement `MessageHandler` with a unique `event_type` and `handle(payload, context)`.
2. Return a `HandlerResult` containing every required `OutgoingRecord`.
3. Register the instance in `bootstrap.py`.

To add a calculation:

1. Implement `CalculationHandler` in `calculations/` with a unique `process_name`.
2. Inject its dependencies through the constructor and implement `calculate(request)`.
3. Register the instance in `CalculationRegistry` in `bootstrap.py`.

Neither extension requires changes to `ConsumerLoop`, `MessageProcessor`, or
`CalculationRequestedMessageHandler`.

## Event contracts

Identifiers are opaque, non-blank strings; they do not need to be UUIDs. Extra input fields are
accepted. An input `occurred_at`, when present, must be timezone-aware. Output `occurred_at` and DLQ
`failed_at` are processing timestamps generated in UTC.

### `calculation.requested`

Input topic: `INTEGRATIONS`.

```json
{
  "event_id": "optional-upstream-id",
  "event_type": "calculation.requested",
  "schema_version": 1,
  "source": "requesting-service",
  "request_id": "request-123",
  "calc_id": "service-456",
  "calc_process": "example",
  "occurred_at": "2026-08-25T12:00:00Z"
}
```

### `calculation.completed`

Output topic: `INTEGRATIONS`; Kafka key: `request_id`.

```json
{
  "event_id": "deterministic-uuid-v5",
  "event_type": "calculation.completed",
  "schema_version": 1,
  "source": "calculation-worker",
  "request_id": "request-123",
  "calc_id": "service-456",
  "calc_process": "example",
  "status": "ok",
  "result": {
    "has_data": true,
    "source_field_count": 4
  },
  "occurred_at": "2026-08-25T12:00:02Z"
}
```

### `calculation.failed`

Output topic: `INTEGRATIONS`; Kafka key: `request_id`. Error messages are sanitized and contain no
stack trace, credentials, token, or upstream response body.

```json
{
  "event_id": "deterministic-uuid-v5",
  "event_type": "calculation.failed",
  "schema_version": 1,
  "source": "calculation-worker",
  "request_id": "request-123",
  "calc_id": "service-456",
  "calc_process": "example",
  "status": "error",
  "error": {
    "code": "UPSTREAM_TIMEOUT",
    "message": "Upstream service did not respond in time",
    "retryable": false
  },
  "occurred_at": "2026-08-25T12:00:32Z"
}
```

### Dead-letter record

Output topic: `INTEGRATIONS.DLQ`; Kafka key: the original key. The original key, value, and every
header value are preserved as Base64 (Kafka header names remain strings), so arbitrary bytes are
safe in JSON.

```json
{
  "source_topic": "INTEGRATIONS",
  "source_partition": 2,
  "source_offset": 152,
  "source_key_base64": "cmVxdWVzdC0xMjM=",
  "source_value_base64": "eyJldmVudF90eXBlIjoiY2FsY3VsYXRpb24ucmVxdWVzdGVkIn0=",
  "source_headers": [
    {
      "name": "trace-id",
      "value_base64": "YWJjLTEyMw=="
    }
  ],
  "error": {
    "code": "INVALID_MESSAGE",
    "message": "Message is not valid JSON"
  },
  "failed_at": "2026-08-25T12:00:00Z",
  "service": "calculation-worker"
}
```

## Delivery and failure policy

The service provides **at-least-once** delivery. Producer idempotence (`enable.idempotence=true`,
`acks=all`) protects producer retries, but it cannot make input processing exactly once. A process
can publish an output and fail before committing the input offset, causing that input and its output
to be produced again. Consumer auto-commit and auto-offset-store are disabled; every successful
input is committed synchronously by the loop.

Completed and failed IDs use UUIDv5 with a fixed namespace and these names:

```text
request_id + ":" + calc_process + ":calculation.completed
request_id + ":" + calc_process + ":calculation.failed
```

The same input therefore produces the same outcome ID, while completed and failed IDs differ.
Downstream consumers must deduplicate by `event_id`. Kafka transactions are intentionally outside
this version; a future consume-transform-produce implementation can use
`send_offsets_to_transaction`.

| Input outcome | Published records | Commit rule |
| --- | --- | --- |
| Unknown valid `event_type` | None | Commit immediately |
| Invalid envelope/JSON/UTF-8 | DLQ | Commit after DLQ acknowledgement |
| Invalid request with all three identifiers | `calculation.failed` + DLQ | Commit after both acknowledgements |
| Invalid request without all identifiers | DLQ | Commit after DLQ acknowledgement |
| Successful calculation | `calculation.completed` | Commit after acknowledgement |
| Unsupported calculation or terminal HTTP/calculation error | `calculation.failed` + DLQ | Commit after both acknowledgements |
| Any producer failure/timeout | Not fully acknowledged | Do not commit; stop with a non-zero exit |

Error codes are `INVALID_MESSAGE`, `UNSUPPORTED_SCHEMA_VERSION`, `UNSUPPORTED_CALCULATION`,
`UPSTREAM_TIMEOUT`, `UPSTREAM_NETWORK_ERROR`, `UPSTREAM_HTTP_ERROR`,
`INVALID_UPSTREAM_RESPONSE`, and `CALCULATION_ERROR`.

HTTP retries use exponential backoff with jitter. Timeouts, network errors, and HTTP
`429/500/502/503/504` are retried; HTTP `400/401/403` and other non-configured statuses are not.
Startup validation ensures the worst-case HTTP retry budget plus Kafka publication time remains
below `KAFKA_MAX_POLL_INTERVAL_MS`.

## Configuration

Copy the example and replace the upstream token and deployment-specific Kafka addresses/identity:

```bash
cp .env.example .env
```

Only the upstream URL and token have no application default. Defaults for other values are useful
locally, but production deployments should set Kafka identity, endpoints, security, timeouts, and
resource limits explicitly. Empty optional SASL/SSL values are treated as unset. Passwords and the
upstream token are secret fields and are excluded from settings representations and logs.

| Variable | Required? / default | Purpose |
| --- | --- | --- |
| `SERVICE_NAME` | Optional: `calculation-worker` | Event source, DLQ service, and log identity |
| `LOG_LEVEL` | Optional: `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL` |
| `KAFKA_BOOTSTRAP_SERVERS` | Optional: `kafka:9092` | Kafka bootstrap brokers |
| `KAFKA_TOPIC` | Optional: `INTEGRATIONS` | Shared input and result topic |
| `KAFKA_DLQ_TOPIC` | Optional: `INTEGRATIONS.DLQ` | Dead-letter topic; must differ from the input topic |
| `KAFKA_GROUP_ID` | Optional: `calculation-worker-v1` | Shared consumer group ID for every replica |
| `KAFKA_CLIENT_ID` | Optional: `calculation-worker-local` | Client ID; make unique per replica |
| `KAFKA_AUTO_OFFSET_RESET` | Optional: `earliest` | `earliest`, `latest`, or `error` |
| `KAFKA_MAX_POLL_INTERVAL_MS` | Optional: `300000` | Maximum interval allowed between polls |
| `KAFKA_SESSION_TIMEOUT_MS` | Optional: `45000` | Consumer group session timeout |
| `KAFKA_DELIVERY_TIMEOUT_MS` | Optional: `30000` | Producer delivery deadline |
| `KAFKA_REQUEST_TIMEOUT_MS` | Optional: `10000` | Producer request timeout; no greater than delivery timeout |
| `KAFKA_POLL_TIMEOUT_SECONDS` | Optional: `1.0` | Bounded blocking poll duration |
| `KAFKA_PUBLISH_TIMEOUT_SECONDS` | Optional: `35.0` | Whole outgoing batch deadline; covers delivery timeout |
| `KAFKA_SHUTDOWN_FLUSH_TIMEOUT_SECONDS` | Optional: `5.0` | Bounded producer flush during shutdown |
| `KAFKA_SECURITY_PROTOCOL` | Optional: `PLAINTEXT` | Kafka security protocol, for example `SASL_SSL` |
| `KAFKA_SASL_MECHANISM` | Conditional | Required with SASL, for example `PLAIN` or `SCRAM-SHA-512` |
| `KAFKA_SASL_USERNAME` | Conditional | Required with SASL |
| `KAFKA_SASL_PASSWORD` | Conditional, secret | Required with SASL |
| `KAFKA_SSL_CA_LOCATION` | Optional, unset | CA bundle path for TLS |
| `UPSTREAM_API_BASE_URL` | **Required** | HTTP(S) base URL without embedded credentials |
| `UPSTREAM_API_PATH_TEMPLATE` | Optional: `/calculations/{calc_id}` | Absolute path containing exactly one `{calc_id}` placeholder |
| `UPSTREAM_API_TOKEN` | **Required**, secret | Bearer token; `replace-me` in the example is not a real credential |
| `UPSTREAM_CONNECT_TIMEOUT_SECONDS` | Optional: `5.0` | HTTP connect timeout per attempt |
| `UPSTREAM_READ_TIMEOUT_SECONDS` | Optional: `30.0` | HTTP read timeout per attempt |
| `UPSTREAM_WRITE_TIMEOUT_SECONDS` | Optional: `10.0` | HTTP write timeout per attempt |
| `UPSTREAM_POOL_TIMEOUT_SECONDS` | Optional: `5.0` | HTTP pool acquisition timeout per attempt |
| `UPSTREAM_MAX_CONNECTIONS` | Optional: `20` | Total pooled HTTP connections |
| `UPSTREAM_MAX_KEEPALIVE_CONNECTIONS` | Optional: `10` | Keep-alive connections; no greater than total connections |
| `UPSTREAM_MAX_ATTEMPTS` | Optional: `3` | Total HTTP attempts, including the first |
| `UPSTREAM_RETRY_MIN_WAIT_SECONDS` | Optional: `0.5` | Minimum retry backoff |
| `UPSTREAM_RETRY_MAX_WAIT_SECONDS` | Optional: `5.0` | Maximum jittered backoff |
| `UPSTREAM_RETRYABLE_STATUS_CODES` | Optional: `429,500,502,503,504` | Comma-separated retryable HTTP statuses |
| `METRICS_PORT` | Optional: `8000` | Prometheus HTTP server port |

`calc_id` is percent-encoded before insertion into the upstream path. One pooled `httpx.Client` is
used for the process lifetime and closed at shutdown.

## Local development

Install [uv](https://docs.astral.sh/uv/), ensure Kafka and the upstream API are reachable, then:

```bash
uv sync
cp .env.example .env
# Edit .env: at minimum replace UPSTREAM_API_TOKEN and local endpoints.
uv run calculation-worker
```

`.python-version` and `pyproject.toml` select Python 3.13. Useful checks are:

```bash
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy src
```

Build a standalone executable for the current operating system and architecture:

```bash
make build
./dist/calculation-worker
```

PyInstaller writes its temporary build files under `build/`; neither build output directory is
committed. Build the executable on the same operating system and architecture where it will run.

## Consumer groups and scaling

Every replica uses the same `KAFKA_GROUP_ID` and a unique `KAFKA_CLIENT_ID`. Each calls
`consumer.subscribe([topic], on_assign=..., on_revoke=...)`; assignment and revocation callbacks log
the affected partitions. The worker never calls `assign()` or derives a partition from a hostname,
pod ordinal, or environment variable. Kafka therefore owns partition balancing and recovery.

Scale by adding replicas with the same group ID. Each instance handles at most one record at a time,
so maximum useful parallelism is the number of `INTEGRATIONS` partitions; additional replicas remain
idle until a rebalance can assign them work.

Inspect the topic and its partition count:

```bash
kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic INTEGRATIONS
```

Inspect group members and assignments:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --group calculation-worker-v1 \
  --members \
  --verbose
```

## Graceful shutdown

`SIGTERM` and `SIGINT` set a stop flag. The loop stops polling for new work; if processing has
already begun, it finishes publication and commits only after all required acknowledgements. A
signal observed after a blocked poll but before processing leaves that record uncommitted for
redelivery. Shutdown closes the consumer, performs a bounded producer flush, closes the long-lived
HTTP client and metrics server, and then exits. There is no unbounded flush or shutdown wait.

## Observability

Logs are JSON on stdout. Depending on the operation they include `event_type`, `request_id`,
`calc_id`, `calc_process`, `topic`, `partition`, `offset`, bounded `kafka_key`, `handler`,
`processing_duration_ms`, `upstream_attempts`, `outcome`, and rebalance partition lists. Stack traces
appear only on error logs. Secrets and full upstream responses are never logged.

Prometheus metrics are exposed at `http://localhost:${METRICS_PORT}/metrics` without an application
HTTP framework:

- `kafka_messages_received_total`
- `kafka_messages_ignored_total`
- `kafka_messages_invalid_total`
- `message_handler_calls_total{handler,status}`
- `message_handler_duration_seconds{handler}`
- `calculations_total{calc_process,status}`
- `calculation_duration_seconds{calc_process}`
- `upstream_requests_total{outcome}`
- `upstream_request_duration_seconds`
- `kafka_publish_total{topic,outcome}`
- `dlq_messages_total`
- `process_last_success_timestamp_seconds`

Request IDs, calculation IDs, offsets, URLs, and exception messages are not metric labels.

## Tests

The default suite is broker-free and uses fake consumer/publisher adapters plus `respx` for HTTP:

```bash
uv run pytest
```

Integration tests are explicitly marked and require a working Docker daemon. They use
`testcontainers` to start Kafka and verify multi-message processing, keys, deterministic event IDs,
and committed-offset behavior:

```bash
uv run pytest -m integration
```

## Container

Build the locked multi-stage image and run it with a read-only root filesystem:

```bash
docker build -t adaptation-ovation .
docker run --rm \
  --read-only \
  --tmpfs /tmp:rw,exec,nosuid,size=128m \
  --env-file .env \
  -p 8000:8000 \
  adaptation-ovation
```

The builder runs the same `make build` target used locally. The runtime is based on
`python:3.13-slim`, receives only the PyInstaller executable from the builder (no source tree,
virtual environment, or build tools), runs as an unprivileged user, and starts it with exec-form
`CMD`. PyInstaller one-file
executables unpack native libraries under `/tmp` at startup, so that explicitly mounted directory
must be writable, executable, and at least 128 MB. When containerized, Kafka and upstream addresses
must be reachable from the container; `localhost` refers to the container itself.
