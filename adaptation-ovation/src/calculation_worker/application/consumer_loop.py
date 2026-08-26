"""Continuous consume-process-publish-commit orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

from calculation_worker.application.contracts import PublisherPort
from calculation_worker.domain.models import MessageContext
from calculation_worker.infrastructure.observability import Metrics
from calculation_worker.settings import Settings

if TYPE_CHECKING:
    from calculation_worker.application.message_processor import MessageProcessor


@dataclass(frozen=True, slots=True)
class ConsumedRecord:
    """A broker-independent view of a consumed Kafka message."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes
    headers: tuple[tuple[str, bytes | None], ...]
    native_message: object = field(repr=False, compare=False)


class ConsumerPort(Protocol):
    """The consumer operations needed by the synchronous worker loop."""

    def subscribe(self, topic: str) -> None: ...

    def poll(self, timeout: float) -> ConsumedRecord | None: ...

    def commit(self, record: ConsumedRecord) -> None: ...

    def close(self) -> None: ...


class StopSignal(Protocol):
    """A signal-safe stop flag, structurally implemented by threading.Event."""

    def is_set(self) -> bool: ...

    def wait(self, timeout: float) -> bool: ...


class ConsumerLoop:
    """Process one Kafka message at a time until stopped or an error escapes."""

    def __init__(
        self,
        consumer: ConsumerPort,
        processor: MessageProcessor,
        publisher: PublisherPort,
        stop_signal: StopSignal,
        settings: Settings,
        metrics: Metrics,
    ) -> None:
        self._consumer = consumer
        self._processor = processor
        self._publisher = publisher
        self._stop_signal = stop_signal
        self._settings = settings
        self._metrics = metrics

    def run(self) -> None:
        """Run continuously; publication and commit failures deliberately escape."""
        self._consumer.subscribe(self._settings.kafka_topic)

        while not self._stop_signal.is_set():
            record = self._consumer.poll(self._settings.kafka_poll_timeout_seconds)

            # A signal can arrive while poll is blocked. Do not start work returned
            # by that poll; closing the consumer leaves it available for redelivery.
            if self._stop_signal.is_set():
                break
            if record is None:
                continue

            self._metrics.kafka_messages_received.inc()
            context = MessageContext(
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                key=record.key,
                headers=record.headers,
                raw_value=record.value,
            )
            result = self._processor.process(record.value, context)
            if result.records:
                self._publisher.publish_and_wait(result.records)

            self._consumer.commit(record)
            self._metrics.process_last_success_timestamp.set_to_current_time()
