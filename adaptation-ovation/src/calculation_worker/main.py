from __future__ import annotations

import logging
import signal
from threading import Event
from types import FrameType

from calculation_worker.bootstrap import Runtime, build_runtime
from calculation_worker.errors import ConfigurationError
from calculation_worker.infrastructure.observability import configure_logging
from calculation_worker.settings import Settings

logger = logging.getLogger(__name__)


def main() -> int:
    configure_logging("INFO")
    try:
        settings = Settings()
    except (ConfigurationError, ValueError):
        logger.error(
            "Service configuration is invalid", extra={"error_code": "CONFIGURATION_ERROR"}
        )
        return 2

    configure_logging(settings.log_level)
    stop_signal = Event()
    _install_signal_handlers(stop_signal)
    runtime: Runtime | None = None
    exit_code = 0
    try:
        runtime = build_runtime(settings, stop_signal)
        logger.info(
            "Calculation worker started",
            extra={
                "service": settings.service_name,
                "topic": settings.kafka_topic,
                "group_id": settings.kafka_group_id,
                "client_id": settings.kafka_client_id,
            },
        )
        runtime.run()
    except Exception:
        exit_code = 1
        logger.exception("Calculation worker stopped after a technical failure")
    finally:
        if runtime is not None and not runtime.close():
            exit_code = 1
        logger.info("Calculation worker stopped", extra={"outcome": "shutdown"})
        logging.shutdown()
    return exit_code


def _install_signal_handlers(stop_signal: Event) -> None:
    def request_shutdown(_signum: int, _frame: FrameType | None) -> None:
        stop_signal.set()

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)


if __name__ == "__main__":
    raise SystemExit(main())
