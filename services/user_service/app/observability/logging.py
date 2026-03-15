import json
import logging
import sys
from typing import Any

from app.config.settings import Settings


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        data: dict[str, Any] = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # optional correlation fields
        for field in ("correlation_id", "request_id", "trace_id", "span_id"):
            value = getattr(record, field, None)
            if value is not None:
                data[field] = value
        return json.dumps(data, ensure_ascii=False)


def setup_logging(settings: Settings) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.setLevel(settings.LOG_LEVEL)
    root.handlers.clear()
    root.addHandler(handler)

