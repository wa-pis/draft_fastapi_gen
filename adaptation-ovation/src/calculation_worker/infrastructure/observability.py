from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from threading import Thread
from traceback import extract_tb, format_list
from typing import Any

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

_LOG_FIELDS = (
    "service",
    "event_type",
    "request_id",
    "calc_id",
    "calc_process",
    "topic",
    "partition",
    "offset",
    "kafka_key",
    "handler",
    "processing_duration_ms",
    "upstream_attempts",
    "outcome",
    "error_code",
    "assigned_partitions",
    "revoked_partitions",
    "group_id",
    "client_id",
    "remaining_messages",
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in _LOG_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        if record.exc_info and record.levelno >= logging.ERROR:
            error_type, _, traceback = record.exc_info
            payload["exception"] = {
                "type": error_type.__name__ if error_type is not None else "Exception",
                "stacktrace": "".join(format_list(extract_tb(traceback))),
            }
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=level, handlers=[handler], force=True)


class Metrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        self.kafka_messages_received = Counter(
            "kafka_messages_received_total",
            "Kafka messages returned by poll",
            registry=self.registry,
        )
        self.kafka_messages_ignored = Counter(
            "kafka_messages_ignored_total",
            "Valid Kafka events without a registered handler",
            registry=self.registry,
        )
        self.kafka_messages_invalid = Counter(
            "kafka_messages_invalid_total",
            "Kafka messages sent to DLQ before handler routing",
            registry=self.registry,
        )
        self.message_handler_calls = Counter(
            "message_handler_calls_total",
            "Message handler calls",
            ("handler", "status"),
            registry=self.registry,
        )
        self.message_handler_duration = Histogram(
            "message_handler_duration_seconds",
            "Message handler duration",
            ("handler",),
            registry=self.registry,
        )
        self.calculations = Counter(
            "calculations_total",
            "Calculation executions",
            ("calc_process", "status"),
            registry=self.registry,
        )
        self.calculation_duration = Histogram(
            "calculation_duration_seconds",
            "Calculation duration",
            ("calc_process",),
            registry=self.registry,
        )
        self.upstream_requests = Counter(
            "upstream_requests_total",
            "Upstream HTTP attempts",
            ("outcome",),
            registry=self.registry,
        )
        self.upstream_request_duration = Histogram(
            "upstream_request_duration_seconds",
            "Upstream HTTP attempt duration",
            registry=self.registry,
        )
        self.kafka_publish = Counter(
            "kafka_publish_total",
            "Kafka records acknowledged by the producer",
            ("topic", "outcome"),
            registry=self.registry,
        )
        self.dlq_messages = Counter(
            "dlq_messages_total",
            "DLQ messages acknowledged by Kafka",
            registry=self.registry,
        )
        self.process_last_success_timestamp = Gauge(
            "process_last_success_timestamp_seconds",
            "Unix timestamp of the last committed input message",
            registry=self.registry,
        )


class MetricsServer:
    def __init__(self, server: Any, thread: Thread) -> None:
        self._server = server
        self._thread = thread

    def close(self, timeout_seconds: float = 2.0) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=timeout_seconds)


def start_metrics(metrics: Metrics, port: int) -> MetricsServer:
    server, thread = start_http_server(port=port, registry=metrics.registry)
    return MetricsServer(server, thread)
