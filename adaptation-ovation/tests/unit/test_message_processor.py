import json
from base64 import b64encode
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import pytest
from prometheus_client import CollectorRegistry

from calculation_worker.application.calculation_requested_handler import (
    CalculationRequestedMessageHandler,
)
from calculation_worker.application.event_factory import EventFactory
from calculation_worker.application.message_processor import MessageProcessor
from calculation_worker.application.registries import MessageHandlerRegistry
from calculation_worker.calculations.registry import CalculationRegistry
from calculation_worker.domain.models import (
    CalculationFailed,
    DeadLetterRecord,
    HandlerResult,
    MessageContext,
)
from calculation_worker.infrastructure.observability import Metrics

FIXED_TIME = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)


class RecordingHandler:
    event_type = "calculation.requested"

    def __init__(self) -> None:
        self.calls: list[tuple[Mapping[str, Any], MessageContext]] = []

    def handle(
        self,
        payload: Mapping[str, Any],
        context: MessageContext,
    ) -> HandlerResult:
        self.calls.append((payload, context))
        return HandlerResult(records=(), outcome="completed")


def _context() -> MessageContext:
    return MessageContext(
        topic="INTEGRATIONS",
        partition=2,
        offset=152,
        key=b"request-123",
        headers=(("trace", b"abc"),),
    )


def _processor(handler: RecordingHandler | None = None) -> MessageProcessor:
    registry = MessageHandlerRegistry()
    if handler is not None:
        registry.register(handler)
    return MessageProcessor(
        message_registry=registry,
        event_factory=EventFactory(
            output_topic="INTEGRATIONS",
            dlq_topic="INTEGRATIONS.DLQ",
            service_name="calculation-worker",
            clock=lambda: FIXED_TIME,
        ),
        metrics=Metrics(CollectorRegistry()),
    )


def test_calculation_requested_is_routed_to_registered_handler() -> None:
    handler = RecordingHandler()
    payload = {
        "event_type": "calculation.requested",
        "schema_version": 1,
        "request_id": "request-123",
        "calc_id": "calc-456",
        "calc_process": "example",
    }
    encoded = json.dumps(payload).encode()

    result = _processor(handler).process(encoded, _context())

    assert result.outcome == "completed"
    assert len(handler.calls) == 1
    routed_payload, routed_context = handler.calls[0]
    assert routed_payload == payload
    assert routed_context.raw_value == encoded


@pytest.mark.parametrize(
    "event_type",
    ["calculation.completed", "calculation.failed", "future.event"],
)
def test_event_without_registered_handler_is_ignored(event_type: str) -> None:
    payload = json.dumps({"event_type": event_type, "schema_version": 1}).encode()

    result = _processor().process(payload, _context())

    assert result == HandlerResult(records=(), outcome="ignored")


def test_invalid_json_is_sent_to_dlq() -> None:
    payload = b'{"event_type":'

    result = _processor().process(payload, _context())

    _assert_invalid_dlq(result, payload)


def test_json_array_is_sent_to_dlq() -> None:
    payload = b'["calculation.requested"]'

    result = _processor().process(payload, _context())

    _assert_invalid_dlq(result, payload)


def test_missing_event_type_is_sent_to_dlq() -> None:
    payload = b'{"schema_version":1,"request_id":"request-123"}'

    result = _processor().process(payload, _context())

    _assert_invalid_dlq(result, payload)


@pytest.mark.parametrize(
    "invalid_field",
    [
        {},
        {"schema_version": None},
        {"schema_version": 1, "occurred_at": "not-a-datetime"},
    ],
)
def test_invalid_known_request_with_identifiers_creates_failed_and_dlq(
    invalid_field: dict[str, object],
) -> None:
    data: dict[str, object] = {
        "event_type": "calculation.requested",
        "request_id": "request-123",
        "calc_id": "calc-456",
        "calc_process": "example",
        **invalid_field,
    }
    result = _calculation_processor().process(json.dumps(data).encode(), _context())

    assert len(result.records) == 2
    assert isinstance(result.records[0].value, CalculationFailed)
    assert isinstance(result.records[1].value, DeadLetterRecord)


def test_invalid_known_request_without_identifiers_creates_only_dlq() -> None:
    payload = b'{"event_type":"calculation.requested"}'

    result = _calculation_processor().process(payload, _context())

    assert len(result.records) == 1
    assert isinstance(result.records[0].value, DeadLetterRecord)


def _assert_invalid_dlq(result: HandlerResult, source_payload: bytes) -> None:
    assert result.outcome == "invalid"
    assert len(result.records) == 1
    record = result.records[0]
    assert record.topic == "INTEGRATIONS.DLQ"
    assert record.key == b"request-123"
    assert isinstance(record.value, DeadLetterRecord)
    assert record.value.error.code == "INVALID_MESSAGE"
    assert record.value.source_value_base64 == b64encode(source_payload).decode("ascii")


def _calculation_processor() -> MessageProcessor:
    metrics = Metrics(CollectorRegistry())
    factory = EventFactory(
        output_topic="INTEGRATIONS",
        dlq_topic="INTEGRATIONS.DLQ",
        service_name="calculation-worker",
        clock=lambda: FIXED_TIME,
    )
    registry = MessageHandlerRegistry()
    registry.register(
        CalculationRequestedMessageHandler(
            calculation_registry=CalculationRegistry(),
            event_factory=factory,
            metrics=metrics,
        )
    )
    return MessageProcessor(registry, factory, metrics)
