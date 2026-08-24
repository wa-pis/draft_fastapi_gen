from __future__ import annotations

import logging
from collections.abc import Mapping
from time import monotonic
from typing import Any

from pydantic import ValidationError

from calculation_worker.application.event_factory import (
    CALCULATION_ERROR,
    INVALID_MESSAGE,
    UNSUPPORTED_SCHEMA_VERSION,
    EventFactory,
    Failure,
    failure_for_exception,
)
from calculation_worker.calculations.registry import CalculationRegistry
from calculation_worker.domain.models import (
    CalculationRequested,
    HandlerResult,
    MessageContext,
)
from calculation_worker.infrastructure.observability import Metrics

logger = logging.getLogger(__name__)


class CalculationRequestedMessageHandler:
    event_type = "calculation.requested"

    def __init__(
        self,
        calculation_registry: CalculationRegistry,
        event_factory: EventFactory,
        metrics: Metrics,
    ) -> None:
        self._calculation_registry = calculation_registry
        self._event_factory = event_factory
        self._metrics = metrics

    def handle(
        self,
        payload: Mapping[str, Any],
        context: MessageContext,
    ) -> HandlerResult:
        try:
            request = CalculationRequested.model_validate(payload)
        except ValidationError:
            self._metrics.kafka_messages_invalid.inc()
            failure = _validation_failure(payload)
            records = [self._event_factory.dead_letter(context, failure)]
            identifiers = _extract_identifiers(payload)
            if identifiers is not None:
                records.insert(0, self._event_factory.failed(*identifiers, failure))
            log_fields: dict[str, object] = {
                **_context_log_fields(context),
                "event_type": self.event_type,
                "handler": type(self).__name__,
                "error_code": failure.code,
                "outcome": "failed",
            }
            if identifiers is not None:
                request_id, calc_id, calc_process = identifiers
                log_fields.update(
                    request_id=request_id,
                    calc_id=calc_id,
                    calc_process=calc_process,
                )
            logger.warning(
                "Calculation request validation failed",
                extra=log_fields,
            )
            return HandlerResult(records=tuple(records), outcome="failed")

        started_at = monotonic()
        metric_process = "unsupported"
        try:
            calculation = self._calculation_registry.get(request.calc_process)
            metric_process = calculation.process_name
            result = calculation.calculate(request)
        except Exception as error:
            duration = monotonic() - started_at
            failure = failure_for_exception(error)
            self._metrics.calculations.labels(metric_process, "failed").inc()
            self._metrics.calculation_duration.labels(metric_process).observe(duration)
            log_fields = {
                **_request_log_fields(request, context),
                "error_code": failure.code,
                "outcome": "failed",
                "processing_duration_ms": round(duration * 1000, 3),
            }
            if failure == CALCULATION_ERROR:
                logger.exception("Unexpected calculation error", extra=log_fields)
            else:
                logger.warning("Calculation failed", extra=log_fields)
            return HandlerResult(
                records=(
                    self._event_factory.failed(
                        request.request_id,
                        request.calc_id,
                        request.calc_process,
                        failure,
                    ),
                    self._event_factory.dead_letter(context, failure),
                ),
                outcome="failed",
            )

        duration = monotonic() - started_at
        self._metrics.calculations.labels(metric_process, "completed").inc()
        self._metrics.calculation_duration.labels(metric_process).observe(duration)
        logger.info(
            "Calculation completed",
            extra={
                **_request_log_fields(request, context),
                "outcome": "completed",
                "processing_duration_ms": round(duration * 1000, 3),
            },
        )
        return HandlerResult(
            records=(self._event_factory.completed(request, result),),
            outcome="completed",
        )


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
        "handler": "CalculationRequestedMessageHandler",
    }


def _safe_key(key: bytes | None) -> str | None:
    if key is None:
        return None
    return key.decode("utf-8", errors="replace")[:256]
