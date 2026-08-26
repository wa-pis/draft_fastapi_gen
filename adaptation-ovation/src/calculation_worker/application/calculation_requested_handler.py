from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from pydantic import ValidationError

from calculation_worker.application.contracts import CalculationQueuePort
from calculation_worker.application.event_factory import (
    INVALID_MESSAGE,
    UNSUPPORTED_SCHEMA_VERSION,
    EventFactory,
    Failure,
)
from calculation_worker.domain.models import (
    CalculationJob,
    CalculationRequested,
    HandlerResult,
    MessageContext,
)
from calculation_worker.infrastructure.observability import Metrics

logger = logging.getLogger(__name__)


class CalculationRequestedIngressHandler:
    """Validate calculation requests and durably enqueue valid ones."""

    event_type = "calculation.requested"

    def __init__(
        self, queue: CalculationQueuePort, event_factory: EventFactory, metrics: Metrics
    ) -> None:
        self._queue = queue
        self._event_factory = event_factory
        self._metrics = metrics

    def handle(self, payload: Mapping[str, Any], context: MessageContext) -> HandlerResult:
        try:
            request = CalculationRequested.model_validate(payload)
        except ValidationError:
            return self._invalid(payload, context)

        workflow_id = self._queue.enqueue(CalculationJob.create(request))
        logger.info(
            "Calculation request durably enqueued",
            extra={
                **_request_log_fields(request, context),
                "workflow_id": workflow_id,
                "outcome": "enqueued",
            },
        )
        return HandlerResult(records=(), outcome="enqueued")

    def _invalid(self, payload: Mapping[str, Any], context: MessageContext) -> HandlerResult:
        self._metrics.kafka_messages_invalid.inc()
        failure = _validation_failure(payload)
        records = []
        identifiers = _extract_identifiers(payload)
        if identifiers is not None:
            records.append(self._event_factory.failed(*identifiers, failure))
        logger.warning(
            "Calculation request validation failed",
            extra={
                **_context_log_fields(context),
                "event_type": self.event_type,
                "handler": type(self).__name__,
                "error_code": failure.code,
                "outcome": "failed",
            },
        )
        return HandlerResult(records=tuple(records), outcome="failed")


def _validation_failure(payload: Mapping[str, Any]) -> Failure:
    if "schema_version" in payload and payload.get("schema_version") != 1:
        return UNSUPPORTED_SCHEMA_VERSION
    return INVALID_MESSAGE


def _extract_identifiers(payload: Mapping[str, Any]) -> tuple[str, str, str] | None:
    values = (payload.get("request_id"), payload.get("calc_id"), payload.get("calc_process"))
    if all(isinstance(value, str) and value.strip() for value in values):
        request_id, calc_id, calc_process = values
        assert isinstance(request_id, str)
        assert isinstance(calc_id, str)
        assert isinstance(calc_process, str)
        return request_id, calc_id, calc_process
    return None


def _context_log_fields(context: MessageContext) -> dict[str, object]:
    return {
        "topic": context.topic,
        "partition": context.partition,
        "offset": context.offset,
        "kafka_key": _safe_key(context.key),
    }


def _request_log_fields(
    request: CalculationRequested, context: MessageContext
) -> dict[str, object]:
    return {
        **_context_log_fields(context),
        "event_type": request.event_type,
        "request_id": request.request_id,
        "calc_id": request.calc_id,
        "calc_process": request.calc_process,
        "handler": "CalculationRequestedIngressHandler",
    }


def _safe_key(key: bytes | None) -> str | None:
    return None if key is None else key.decode("utf-8", errors="replace")[:256]
