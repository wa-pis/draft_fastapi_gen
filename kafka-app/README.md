### Async Kafka Service

Production-like, intentionally small Python service for asynchronous Kafka message processing.
It uses `asyncio`, `aiokafka`, Pydantic settings and schemas, JSON logs, OpenTelemetry tracing,
manual Kafka offset commits, and graceful shutdown.

## Project structure

```text
.
├── README.md
├── .env.example
├── pyproject.toml
└── src
    └── app
        ├── __init__.py
        ├── main.py
        ├── config.py
        ├── logging_config.py
        ├── kafka
        │   ├── __init__.py
        │   ├── consumer.py
        │   └── producer.py
        ├── handlers
        │   ├── __init__.py
        │   └── event_handler.py
        ├── schemas
        │   ├── __init__.py
        │   └── events.py
        ├── services
        │   ├── __init__.py
        │   └── event_service.py
        └── observability
            ├── __init__.py
            ├── tracing.py
            └── context.py
```

## Configuration

All settings are read from environment variables or `.env` via `pydantic-settings`.

```env
APP_NAME=kafka-async-service
APP_ENV=local
LOG_LEVEL=INFO

KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_GROUP_ID=kafka-async-service-group
KAFKA_INPUT_TOPIC=input-topic
KAFKA_OUTPUT_TOPIC=output-topic

REMOTE_LOG_ENABLED=true
REMOTE_LOG_HOST=localhost
REMOTE_LOG_PORT=5000
REMOTE_LOG_PROTOCOL=tcp

OTEL_ENABLED=true
OTEL_SERVICE_NAME=kafka-async-service
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
OTEL_EXPORTER_OTLP_HEADERS=
OTEL_TRACES_SAMPLER=parentbased_traceidratio
OTEL_TRACES_SAMPLER_ARG=1.0
```

`KAFKA_OUTPUT_TOPIC` enables the producer path. Set it to an empty value if you do not want result
messages.

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
cp .env.example .env
```

## Run locally

Start Kafka and, optionally, an OpenTelemetry Collector. Then run:

```bash
PYTHONPATH=src python -m app.main
```

The consumer reads from `KAFKA_INPUT_TOPIC` with `enable_auto_commit=false`. It commits the offset
only after the message is decoded, validated, handled by the service layer, and the optional output
message is sent successfully. On decoding, validation, business, producer, or Kafka errors, the error
is logged and the offset is not committed.

## Example Kafka message

```json
{
  "event_id": "e7b44b3e-4e5c-4f53-b4c6-75fd7c187a11",
  "event_type": "user.created",
  "correlation_id": "corr-123",
  "payload": {
    "user_id": "123",
    "email": "user@example.com"
  }
}
```

## JSON logs

Logs go to `stdout` as JSON and include:

- `timestamp`
- `level`
- `app_name`
- `environment`
- `logger`
- `message`
- `correlation_id`
- `trace_id`
- `span_id`
- `exception.type`, `exception.message`, `exception.stacktrace` on errors

If `REMOTE_LOG_ENABLED=true`, the same JSON payload is also sent to `REMOTE_LOG_HOST:REMOTE_LOG_PORT`
using TCP or UDP. Remote logging is best-effort: network failures are swallowed by the handler and do
not interrupt message processing.

## OpenTelemetry

Tracing is configured in `src/app/observability/tracing.py`. When `OTEL_ENABLED=true`, spans are
exported to the OTLP endpoint configured by `OTEL_EXPORTER_OTLP_ENDPOINT`, for example an
OpenTelemetry Collector listening on `http://localhost:4317`.

Minimal collector receiver/exporter configuration:

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

processors:
  batch:

exporters:
  debug:
    verbosity: detailed

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
```

For each incoming Kafka message, the service creates a processing span with Kafka attributes,
`correlation_id`, and `message_type`. Handler, service, and producer calls create nested spans. Logs
read `trace_id` and `span_id` from the active OpenTelemetry context, which lets logs and traces be
correlated in a backend.

## Graceful shutdown

`SIGINT` and `SIGTERM` set an application stop event. Shutdown then:

1. Cancels the consumer task.
2. Stops the Kafka consumer.
3. Stops the Kafka producer.
4. Cancels remaining pending asyncio tasks.
5. Shuts down the OpenTelemetry tracer provider so pending spans can be exported.
6. Flushes and closes logging handlers.

## Retry policy

The service layer uses `tenacity` to retry `BusinessLogicError` up to three times with exponential
backoff. The consumer contains a clear extension point for a dead-letter topic or a richer poison
message policy.
