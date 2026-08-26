import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import pytest
from prometheus_client import CollectorRegistry

from calculation_worker.application.calculation_requested_handler import (
    CalculationRequestedIngressHandler,
)
from calculation_worker.application.event_factory import EventFactory
from calculation_worker.application.message_processor import MessageProcessor
from calculation_worker.application.registries import MessageHandlerRegistry
from calculation_worker.domain.models import (
    CalculationFailed,
    CalculationJob,
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


class RejectingQueue:
    def enqueue(self, _job: CalculationJob) -> str:
        raise AssertionError("invalid requests must not be enqueued")


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
    ["calculation.started", "calculation.completed", "calculation.failed", "future.event"],
)
def test_event_without_registered_handler_is_ignored(event_type: str) -> None:
    payload = json.dumps({"event_type": event_type, "schema_version": 1}).encode()

    result = _processor().process(payload, _context())

    assert result == HandlerResult(records=(), outcome="ignored")


def test_invalid_json_is_discarded() -> None:
    payload = b'{"event_type":'

    result = _processor().process(payload, _context())

    assert result == HandlerResult(records=(), outcome="invalid")


def test_json_array_is_discarded() -> None:
    payload = b'["calculation.requested"]'

    result = _processor().process(payload, _context())

    assert result == HandlerResult(records=(), outcome="invalid")


def test_missing_event_type_is_discarded() -> None:
    payload = b'{"schema_version":1,"request_id":"request-123"}'

    result = _processor().process(payload, _context())

    assert result == HandlerResult(records=(), outcome="invalid")


@pytest.mark.parametrize(
    "invalid_field",
    [
        {},
        {"schema_version": None},
        {"schema_version": 1, "occurred_at": "not-a-datetime"},
    ],
)
def test_invalid_known_request_with_identifiers_creates_failed_event(
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

    assert len(result.records) == 1
    assert isinstance(result.records[0].value, CalculationFailed)


def test_invalid_known_request_without_identifiers_is_discarded() -> None:
    payload = b'{"event_type":"calculation.requested"}'

    result = _calculation_processor().process(payload, _context())

    assert result.records == ()


def _calculation_processor() -> MessageProcessor:
    metrics = Metrics(CollectorRegistry())
    factory = EventFactory(
        output_topic="INTEGRATIONS",
        service_name="calculation-worker",
        clock=lambda: FIXED_TIME,
    )
    registry = MessageHandlerRegistry()
    registry.register(
        CalculationRequestedIngressHandler(
            queue=RejectingQueue(),
            event_factory=factory,
            metrics=metrics,
        )
    )
    return MessageProcessor(registry, factory, metrics)
