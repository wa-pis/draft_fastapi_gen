from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from prometheus_client import CollectorRegistry

from calculation_worker.application.calculation_requested_handler import (
    CalculationRequestedMessageHandler,
)
from calculation_worker.application.event_factory import EventFactory, Failure
from calculation_worker.calculations.base import CalculationHandler
from calculation_worker.calculations.example import ExampleCalculation
from calculation_worker.calculations.registry import CalculationRegistry
from calculation_worker.domain.models import (
    CalculationCompleted,
    CalculationFailed,
    CalculationRequested,
    CalculationResult,
    DeadLetterRecord,
    MessageContext,
)
from calculation_worker.infrastructure.observability import Metrics

FIXED_TIME = datetime(2026, 8, 25, 12, 0, 2, tzinfo=UTC)
VALID_PAYLOAD: dict[str, Any] = {
    "event_type": "calculation.requested",
    "schema_version": 1,
    "request_id": "request-123",
    "calc_id": "service-456",
    "calc_process": "example",
    "extra_field": "forward-compatible",
}


class StaticUpstream:
    def get_data(self, calc_id: str) -> Mapping[str, Any]:
        assert calc_id == "service-456"
        return {"one": 1, "two": 2, "three": 3, "four": 4}


class ExplodingCalculation:
    process_name = "example"

    def calculate(self, request: CalculationRequested) -> CalculationResult:
        raise RuntimeError("Bearer extremely-secret-token")


def _context() -> MessageContext:
    return MessageContext(
        topic="INTEGRATIONS",
        partition=0,
        offset=7,
        key=b"request-123",
        headers=(),
        raw_value=b"source bytes",
    )


def _factory() -> EventFactory:
    return EventFactory(
        output_topic="INTEGRATIONS",
        dlq_topic="INTEGRATIONS.DLQ",
        service_name="calculation-worker",
        clock=lambda: FIXED_TIME,
    )


def _handler(
    calculation: CalculationHandler | None = None,
) -> CalculationRequestedMessageHandler:
    registry = CalculationRegistry()
    if calculation is not None:
        registry.register(calculation)
    return CalculationRequestedMessageHandler(
        calculation_registry=registry,
        event_factory=_factory(),
        metrics=Metrics(CollectorRegistry()),
    )


def test_valid_request_creates_completed_event_with_request_key() -> None:
    result = _handler(ExampleCalculation(StaticUpstream())).handle(VALID_PAYLOAD, _context())

    assert result.outcome == "completed"
    assert len(result.records) == 1
    record = result.records[0]
    assert record.key == "request-123"
    assert isinstance(record.value, CalculationCompleted)
    assert record.value.request_id == "request-123"
    assert record.value.calc_id == "service-456"
    assert record.value.result == {"has_data": True, "source_field_count": 4}


def test_reprocessing_request_creates_same_completed_event_id() -> None:
    handler = _handler(ExampleCalculation(StaticUpstream()))

    first = handler.handle(VALID_PAYLOAD, _context()).records[0].value
    second = handler.handle(VALID_PAYLOAD, _context()).records[0].value

    assert isinstance(first, CalculationCompleted)
    assert isinstance(second, CalculationCompleted)
    assert first.event_id == second.event_id


def test_completed_and_failed_events_have_different_ids() -> None:
    factory = _factory()
    request = CalculationRequested.model_validate(VALID_PAYLOAD)
    completed = factory.completed(request, CalculationResult(data={})).value
    failed = factory.failed(
        request.request_id,
        request.calc_id,
        request.calc_process,
        Failure("CALCULATION_ERROR", "Calculation failed"),
    ).value

    assert isinstance(completed, CalculationCompleted)
    assert isinstance(failed, CalculationFailed)
    assert completed.event_id != failed.event_id


def test_unknown_calculation_creates_failed_event_and_dlq() -> None:
    result = _handler().handle(VALID_PAYLOAD, _context())

    assert result.outcome == "failed"
    assert len(result.records) == 2
    failed, dead_letter = (record.value for record in result.records)
    assert isinstance(failed, CalculationFailed)
    assert failed.error.code == "UNSUPPORTED_CALCULATION"
    assert isinstance(dead_letter, DeadLetterRecord)
    assert dead_letter.error.code == "UNSUPPORTED_CALCULATION"


def test_invalid_schema_with_identifiers_creates_failed_and_dlq() -> None:
    payload = {**VALID_PAYLOAD, "schema_version": 2}

    result = _handler(ExampleCalculation(StaticUpstream())).handle(payload, _context())

    assert len(result.records) == 2
    failed, dead_letter = (record.value for record in result.records)
    assert isinstance(failed, CalculationFailed)
    assert failed.error.code == "UNSUPPORTED_SCHEMA_VERSION"
    assert isinstance(dead_letter, DeadLetterRecord)
    assert dead_letter.error.code == "UNSUPPORTED_SCHEMA_VERSION"


def test_invalid_request_without_all_identifiers_creates_only_dlq() -> None:
    payload = {key: value for key, value in VALID_PAYLOAD.items() if key != "request_id"}

    result = _handler(ExampleCalculation(StaticUpstream())).handle(payload, _context())

    assert len(result.records) == 1
    assert isinstance(result.records[0].value, DeadLetterRecord)
    assert result.records[0].value.error.code == "INVALID_MESSAGE"


def test_invalid_request_increments_invalid_message_metric() -> None:
    metrics = Metrics(CollectorRegistry())
    handler = CalculationRequestedMessageHandler(
        calculation_registry=CalculationRegistry(),
        event_factory=_factory(),
        metrics=metrics,
    )

    handler.handle({"event_type": "calculation.requested"}, _context())

    sample = next(
        sample
        for metric in metrics.kafka_messages_invalid.collect()
        for sample in metric.samples
        if sample.name == "kafka_messages_invalid_total"
    )
    assert sample.value == 1


def test_unexpected_calculation_error_is_sanitized() -> None:
    secret = "extremely-secret-token"

    result = _handler(ExplodingCalculation()).handle(VALID_PAYLOAD, _context())

    assert len(result.records) == 2
    failed, dead_letter = (record.value for record in result.records)
    assert isinstance(failed, CalculationFailed)
    assert failed.error.code == "CALCULATION_ERROR"
    assert failed.error.message == "Calculation failed"
    assert secret not in failed.model_dump_json()
    assert isinstance(dead_letter, DeadLetterRecord)
    assert dead_letter.error.code == "CALCULATION_ERROR"
    assert secret not in dead_letter.model_dump_json()
