from __future__ import annotations

from datetime import UTC, datetime

from prometheus_client import CollectorRegistry

from calculation_worker.application.calculation_requested_handler import (
    CalculationRequestedIngressHandler,
)
from calculation_worker.application.event_factory import EventFactory
from calculation_worker.domain.models import (
    CalculationFailed,
    CalculationJob,
    MessageContext,
)
from calculation_worker.infrastructure.observability import Metrics

VALID_PAYLOAD = {
    "event_type": "calculation.requested",
    "schema_version": 1,
    "request_id": "request-123",
    "calc_id": "service-456",
    "calc_process": "example",
}


class RecordingQueue:
    def __init__(self) -> None:
        self.jobs: list[CalculationJob] = []

    def enqueue(self, job: CalculationJob) -> str:
        self.jobs.append(job)
        return "workflow-123"


def _context() -> MessageContext:
    return MessageContext(
        topic="INTEGRATIONS",
        partition=2,
        offset=7,
        key=b"request-123",
        headers=(("trace-id", b"abc"),),
        raw_value=b"original bytes",
    )


def _handler(queue: RecordingQueue) -> CalculationRequestedIngressHandler:
    return CalculationRequestedIngressHandler(
        queue,
        EventFactory(
            "INTEGRATIONS",
            "calculation-worker",
            clock=lambda: datetime(2026, 8, 25, tzinfo=UTC),
        ),
        Metrics(CollectorRegistry()),
    )


def test_valid_request_is_enqueued_without_lifecycle_publication() -> None:
    queue = RecordingQueue()

    result = _handler(queue).handle(VALID_PAYLOAD, _context())

    assert result.outcome == "enqueued"
    assert result.records == ()
    assert len(queue.jobs) == 1
    job = queue.jobs[0]
    assert job.request.request_id == "request-123"
    assert job.model_dump(mode="json") == {"request": job.request.model_dump(mode="json")}


def test_invalid_schema_with_identifiers_creates_failed_event() -> None:
    queue = RecordingQueue()

    result = _handler(queue).handle({**VALID_PAYLOAD, "schema_version": 2}, _context())

    assert queue.jobs == []
    assert result.outcome == "failed"
    assert len(result.records) == 1
    assert isinstance(result.records[0].value, CalculationFailed)
    assert result.records[0].value.error.code == "UNSUPPORTED_SCHEMA_VERSION"


def test_invalid_request_without_identifiers_is_discarded() -> None:
    queue = RecordingQueue()

    result = _handler(queue).handle({"event_type": "calculation.requested"}, _context())

    assert queue.jobs == []
    assert result.records == ()
