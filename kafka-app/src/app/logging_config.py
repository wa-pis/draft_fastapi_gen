import json
import logging
import socket
import sys
import traceback
from datetime import UTC, datetime
from logging import Handler, LogRecord
from typing import Any

from app.config import AppSettings, LoggingSettings
from app.observability.context import get_correlation_id, get_current_trace_ids


class JsonLogFormatter(logging.Formatter):
    def __init__(self, app_settings: AppSettings) -> None:
        super().__init__()
        self._app_settings = app_settings

    def format(self, record: LogRecord) -> str:
        trace_id, span_id = get_current_trace_ids()
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "app_name": self._app_settings.name,
            "environment": self._app_settings.environment,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": get_correlation_id(),
            "trace_id": trace_id,
            "span_id": span_id,
        }

        for key, value in record.__dict__.items():
            if key.startswith("_") or key in _RESERVED_LOG_RECORD_KEYS:
                continue
            payload[key] = value

        if record.exc_info:
            exc_type, exc_value, exc_tb = record.exc_info
            payload["exception"] = {
                "type": exc_type.__name__ if exc_type else None,
                "message": str(exc_value),
                "stacktrace": "".join(traceback.format_exception(exc_type, exc_value, exc_tb)),
            }

        return json.dumps(payload, ensure_ascii=False, default=str)


class RemoteJsonLogHandler(Handler):
    """Best-effort network log handler that never raises into application code."""

    def __init__(self, settings: LoggingSettings) -> None:
        super().__init__()
        self._settings = settings

    def emit(self, record: LogRecord) -> None:
        try:
            message = self.format(record).encode("utf-8") + b"\n"
            if self._settings.remote_protocol == "udp":
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                    sock.settimeout(self._settings.remote_timeout_seconds)
                    sock.sendto(message, (self._settings.remote_host, self._settings.remote_port))
                return

            with socket.create_connection(
                (self._settings.remote_host, self._settings.remote_port),
                timeout=self._settings.remote_timeout_seconds,
            ) as sock:
                sock.sendall(message)
        except Exception:
            self.handleError(record)

    def handleError(self, record: LogRecord) -> None:
        # The remote logging endpoint must not affect message processing.
        return


def configure_logging(app_settings: AppSettings, logging_settings: LoggingSettings) -> None:
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging_settings.level.upper())

    formatter = JsonLogFormatter(app_settings)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    root_logger.addHandler(stdout_handler)

    if logging_settings.remote_enabled:
        remote_handler = RemoteJsonLogHandler(logging_settings)
        remote_handler.setFormatter(formatter)
        root_logger.addHandler(remote_handler)


_RESERVED_LOG_RECORD_KEYS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
}
