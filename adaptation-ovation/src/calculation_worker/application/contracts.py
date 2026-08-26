"""Application-facing structural interfaces."""

from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from calculation_worker.domain.models import (
    CalculationJob,
    HandlerResult,
    MessageContext,
    OutgoingRecord,
)


class PublisherPort(Protocol):
    """Synchronously publish records or raise before processing continues."""

    def publish_and_wait(self, records: Sequence[OutgoingRecord]) -> None: ...


class CalculationQueuePort(Protocol):
    """Durably enqueue a validated calculation before Kafka is committed."""

    def enqueue(self, job: CalculationJob) -> str: ...


class MessageHandler(Protocol):
    """Handle one routed Kafka event without committing its input offset."""

    event_type: str

    def handle(
        self,
        payload: Mapping[str, Any],
        context: MessageContext,
    ) -> HandlerResult:
        """Return records that remain to be published before input commit."""
        ...
