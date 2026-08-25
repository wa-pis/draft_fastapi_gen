from __future__ import annotations

from base64 import b64encode
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid5

from calculation_worker.domain.models import (
    CalculationCompleted,
    CalculationFailed,
    CalculationRequested,
    CalculationResult,
    CalculationStarted,
    DeadLetterError,
    DeadLetterHeader,
    DeadLetterRecord,
    EventError,
    MessageContext,
    OutgoingRecord,
)
from calculation_worker.errors import (
    InvalidUpstreamResponseError,
    UnsupportedCalculationError,
    UpstreamHttpError,
    UpstreamNetworkError,
    UpstreamTimeoutError,
)

EVENT_ID_NAMESPACE = UUID("36fd3a66-b290-5d15-8699-d253009aa30f")


@dataclass(frozen=True, slots=True)
class Failure:
    code: str
    message: str


INVALID_MESSAGE = Failure("INVALID_MESSAGE", "Message does not match the required schema")
UNSUPPORTED_SCHEMA_VERSION = Failure(
    "UNSUPPORTED_SCHEMA_VERSION", "Unsupported calculation.requested schema version"
)
UNSUPPORTED_CALCULATION = Failure(
    "UNSUPPORTED_CALCULATION", "No calculation handler is registered for calc_process"
)
UPSTREAM_TIMEOUT = Failure("UPSTREAM_TIMEOUT", "Upstream service did not respond in time")
UPSTREAM_NETWORK_ERROR = Failure("UPSTREAM_NETWORK_ERROR", "Upstream service could not be reached")
INVALID_UPSTREAM_RESPONSE = Failure(
    "INVALID_UPSTREAM_RESPONSE", "Upstream service returned an invalid JSON object"
)
CALCULATION_ERROR = Failure("CALCULATION_ERROR", "Calculation failed")


def failure_for_exception(error: Exception) -> Failure:
    if isinstance(error, UnsupportedCalculationError):
        return UNSUPPORTED_CALCULATION
    if isinstance(error, UpstreamTimeoutError):
        return UPSTREAM_TIMEOUT
    if isinstance(error, UpstreamNetworkError):
        return UPSTREAM_NETWORK_ERROR
    if isinstance(error, UpstreamHttpError):
        return Failure("UPSTREAM_HTTP_ERROR", f"Upstream service returned HTTP {error.status_code}")
    if isinstance(error, InvalidUpstreamResponseError):
        return INVALID_UPSTREAM_RESPONSE
    return CALCULATION_ERROR


class EventFactory:
    def __init__(
        self,
        output_topic: str,
        dlq_topic: str,
        service_name: str,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.output_topic = output_topic
        self.dlq_topic = dlq_topic
        self._service_name = service_name
        self._clock = clock or (lambda: datetime.now(UTC))

    def started(self, request: CalculationRequested) -> OutgoingRecord:
        value = CalculationStarted(
            event_id=_event_id(request.request_id, request.calc_process, "calculation.started"),
            source=self._service_name,
            request_id=request.request_id,
            calc_id=request.calc_id,
            calc_process=request.calc_process,
            occurred_at=self._clock(),
        )
        return OutgoingRecord(topic=self.output_topic, key=request.request_id, value=value)

    def completed(self, request: CalculationRequested, result: CalculationResult) -> OutgoingRecord:
        value = CalculationCompleted(
            event_id=_event_id(request.request_id, request.calc_process, "calculation.completed"),
            source=self._service_name,
            request_id=request.request_id,
            calc_id=request.calc_id,
            calc_process=request.calc_process,
            result=result.data,
            occurred_at=self._clock(),
        )
        return OutgoingRecord(topic=self.output_topic, key=request.request_id, value=value)

    def failed(
        self,
        request_id: str,
        calc_id: str,
        calc_process: str,
        failure: Failure,
    ) -> OutgoingRecord:
        value = CalculationFailed(
            event_id=_event_id(request_id, calc_process, "calculation.failed"),
            source=self._service_name,
            request_id=request_id,
            calc_id=calc_id,
            calc_process=calc_process,
            error=EventError(code=failure.code, message=failure.message, retryable=False),
            occurred_at=self._clock(),
        )
        return OutgoingRecord(topic=self.output_topic, key=request_id, value=value)

    def dead_letter(self, context: MessageContext, failure: Failure) -> OutgoingRecord:
        value = DeadLetterRecord(
            source_topic=context.topic,
            source_partition=context.partition,
            source_offset=context.offset,
            source_key_base64=_optional_base64(context.key),
            source_value_base64=_base64(context.raw_value),
            source_headers=tuple(
                DeadLetterHeader(name=name, value_base64=_optional_base64(header_value))
                for name, header_value in context.headers
            ),
            error=DeadLetterError(code=failure.code, message=failure.message),
            failed_at=self._clock(),
            service=self._service_name,
        )
        return OutgoingRecord(topic=self.dlq_topic, key=context.key, value=value)


def _event_id(request_id: str, calc_process: str, event_type: str) -> str:
    return str(uuid5(EVENT_ID_NAMESPACE, f"{request_id}:{calc_process}:{event_type}"))


def _base64(value: bytes) -> str:
    return b64encode(value).decode("ascii")


def _optional_base64(value: bytes | None) -> str | None:
    return None if value is None else _base64(value)
