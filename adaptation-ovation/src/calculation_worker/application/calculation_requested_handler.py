from __future__ import annotations

import logging
from collections.abc import Mapping
from time import monotonic
from typing import Any

from pydantic import ValidationError

from calculation_worker.application.contracts import PublisherPort
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
        publisher: PublisherPort,
    ) -> None:
        self._calculation_registry = calculation_registry
        self._event_factory = event_factory
        self._metrics = metrics
        self._publisher = publisher

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

        lookup_started_at = monotonic()
        try:
            calculation = self._calculation_registry.get(request.calc_process)
        except Exception as error:
            return self._failed(
                request,
                context,
                metric_process="unsupported",
                started_at=lookup_started_at,
                error=error,
            )

        self._publisher.publish_and_wait((self._event_factory.started(request),))
        logger.info(
            "Calculation started",
            extra={**_request_log_fields(request, context), "outcome": "started"},
        )

        started_at = monotonic()
        try:
            result = calculation.calculate(request)
        except Exception as error:
            return self._failed(
                request,
                context,
                metric_process=calculation.process_name,
                started_at=started_at,
                error=error,
            )

        duration = monotonic() - started_at
        self._metrics.calculations.labels(calculation.process_name, "completed").inc()
        self._metrics.calculation_duration.labels(calculation.process_name).observe(duration)
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

    def _failed(
        self,
        request: CalculationRequested,
        context: MessageContext,
        metric_process: str,
        started_at: float,
        error: Exception,
    ) -> HandlerResult:
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
