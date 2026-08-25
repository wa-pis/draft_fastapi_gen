from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

import pytest
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
    CalculationStarted,
    DeadLetterRecord,
    MessageContext,
    OutgoingRecord,
)
from calculation_worker.errors import PublishError
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


class RecordingCalculation:
    process_name = "example"

    def __init__(self, timeline: list[str]) -> None:
        self._timeline = timeline

    def calculate(self, _request: CalculationRequested) -> CalculationResult:
        self._timeline.append("calculate")
        return CalculationResult(data={"ok": True})


class RecordingPublisher:
    def __init__(self, timeline: list[str] | None = None, *, fail: bool = False) -> None:
        self._timeline = timeline
        self._fail = fail
        self.batches: list[tuple[OutgoingRecord, ...]] = []

    def publish_and_wait(self, records: Sequence[OutgoingRecord]) -> None:
        if self._fail:
            if self._timeline is not None:
                self._timeline.append("started_failed")
            raise PublishError("started publication failed")
        batch = tuple(records)
        self.batches.append(batch)
        if self._timeline is not None:
            self._timeline.append("started_ack")


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
    publisher: RecordingPublisher | None = None,
) -> CalculationRequestedMessageHandler:
    registry = CalculationRegistry()
    if calculation is not None:
        registry.register(calculation)
    return CalculationRequestedMessageHandler(
        calculation_registry=registry,
        event_factory=_factory(),
        metrics=Metrics(CollectorRegistry()),
        publisher=publisher or RecordingPublisher(),
    )


def test_valid_request_creates_completed_event_with_request_key() -> None:
    publisher = RecordingPublisher()

    result = _handler(ExampleCalculation(StaticUpstream()), publisher).handle(
        VALID_PAYLOAD, _context()
    )

    assert result.outcome == "completed"
    assert len(publisher.batches) == 1
    started_record = publisher.batches[0][0]
    assert started_record.key == "request-123"
    assert isinstance(started_record.value, CalculationStarted)
    assert started_record.value.request_id == "request-123"
    assert started_record.value.calc_id == "service-456"
    assert started_record.value.calc_process == "example"
    assert started_record.value.status == "started"
    assert len(result.records) == 1
    record = result.records[0]
    assert record.key == "request-123"
    assert isinstance(record.value, CalculationCompleted)
    assert record.value.request_id == "request-123"
    assert record.value.calc_id == "service-456"
    assert record.value.result == {"has_data": True, "source_field_count": 4}


def test_reprocessing_request_creates_same_completed_event_id() -> None:
    publisher = RecordingPublisher()
    handler = _handler(ExampleCalculation(StaticUpstream()), publisher)

    first = handler.handle(VALID_PAYLOAD, _context()).records[0].value
    second = handler.handle(VALID_PAYLOAD, _context()).records[0].value

    assert isinstance(first, CalculationCompleted)
    assert isinstance(second, CalculationCompleted)
    assert first.event_id == second.event_id
    first_started = publisher.batches[0][0].value
    second_started = publisher.batches[1][0].value
    assert isinstance(first_started, CalculationStarted)
    assert isinstance(second_started, CalculationStarted)
    assert first_started.event_id == second_started.event_id


def test_completed_and_failed_events_have_different_ids() -> None:
    factory = _factory()
    request = CalculationRequested.model_validate(VALID_PAYLOAD)
    started = factory.started(request).value
    completed = factory.completed(request, CalculationResult(data={})).value
    failed = factory.failed(
        request.request_id,
        request.calc_id,
        request.calc_process,
        Failure("CALCULATION_ERROR", "Calculation failed"),
    ).value

    assert isinstance(started, CalculationStarted)
    assert isinstance(completed, CalculationCompleted)
    assert isinstance(failed, CalculationFailed)
    assert len({started.event_id, completed.event_id, failed.event_id}) == 3


def test_started_is_acknowledged_before_calculation_runs() -> None:
    timeline: list[str] = []

    result = _handler(RecordingCalculation(timeline), RecordingPublisher(timeline)).handle(
        VALID_PAYLOAD, _context()
    )

    assert result.outcome == "completed"
    assert timeline == ["started_ack", "calculate"]


def test_started_publish_failure_prevents_calculation() -> None:
    timeline: list[str] = []
    handler = _handler(RecordingCalculation(timeline), RecordingPublisher(timeline, fail=True))

    with pytest.raises(PublishError, match="started publication failed"):
        handler.handle(VALID_PAYLOAD, _context())

    assert timeline == ["started_failed"]


def test_unknown_calculation_creates_failed_event_and_dlq() -> None:
    publisher = RecordingPublisher()

    result = _handler(publisher=publisher).handle(VALID_PAYLOAD, _context())

    assert publisher.batches == []
    assert result.outcome == "failed"
    assert len(result.records) == 2
    failed, dead_letter = (record.value for record in result.records)
    assert isinstance(failed, CalculationFailed)
    assert failed.error.code == "UNSUPPORTED_CALCULATION"
    assert isinstance(dead_letter, DeadLetterRecord)
    assert dead_letter.error.code == "UNSUPPORTED_CALCULATION"


def test_invalid_schema_with_identifiers_creates_failed_and_dlq() -> None:
    payload = {**VALID_PAYLOAD, "schema_version": 2}
    publisher = RecordingPublisher()

    result = _handler(ExampleCalculation(StaticUpstream()), publisher).handle(payload, _context())

    assert publisher.batches == []
    assert len(result.records) == 2
    failed, dead_letter = (record.value for record in result.records)
    assert isinstance(failed, CalculationFailed)
    assert failed.error.code == "UNSUPPORTED_SCHEMA_VERSION"
    assert isinstance(dead_letter, DeadLetterRecord)
    assert dead_letter.error.code == "UNSUPPORTED_SCHEMA_VERSION"


def test_invalid_request_without_all_identifiers_creates_only_dlq() -> None:
    payload = {key: value for key, value in VALID_PAYLOAD.items() if key != "request_id"}
    publisher = RecordingPublisher()

    result = _handler(ExampleCalculation(StaticUpstream()), publisher).handle(payload, _context())

    assert publisher.batches == []
    assert len(result.records) == 1
    assert isinstance(result.records[0].value, DeadLetterRecord)
    assert result.records[0].value.error.code == "INVALID_MESSAGE"


def test_invalid_request_increments_invalid_message_metric() -> None:
    metrics = Metrics(CollectorRegistry())
    handler = CalculationRequestedMessageHandler(
        calculation_registry=CalculationRegistry(),
        event_factory=_factory(),
        metrics=metrics,
        publisher=RecordingPublisher(),
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
    publisher = RecordingPublisher()

    result = _handler(ExplodingCalculation(), publisher).handle(VALID_PAYLOAD, _context())

    assert len(publisher.batches) == 1
    assert isinstance(publisher.batches[0][0].value, CalculationStarted)
    assert len(result.records) == 2
    failed, dead_letter = (record.value for record in result.records)
    assert isinstance(failed, CalculationFailed)
    assert failed.error.code == "CALCULATION_ERROR"
    assert failed.error.message == "Calculation failed"
    assert secret not in failed.model_dump_json()
    assert isinstance(dead_letter, DeadLetterRecord)
    assert dead_letter.error.code == "CALCULATION_ERROR"
    assert secret not in dead_letter.model_dump_json()
