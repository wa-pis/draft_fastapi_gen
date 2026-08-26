"""Synchronous confluent-kafka adapters."""

from __future__ import annotations

import json
import logging
import time
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from confluent_kafka import (
    Consumer,
    KafkaError,
    Message,
    Producer,
    TopicPartition,
)
from pydantic import BaseModel

from calculation_worker.application.consumer_loop import ConsumedRecord
from calculation_worker.domain.models import OutgoingRecord
from calculation_worker.errors import ConsumeError, PublishError
from calculation_worker.infrastructure.observability import Metrics
from calculation_worker.settings import Settings

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _PreparedRecord:
    topic: str
    key: bytes | None
    value: bytes
    headers: list[tuple[str, str | bytes | None]]


class KafkaConsumerAdapter:
    """Translate confluent-kafka messages into application records."""

    def __init__(
        self,
        settings: Settings,
        native_consumer: Consumer | None = None,
    ) -> None:
        self._consumer = (
            native_consumer
            if native_consumer is not None
            else Consumer(cast(dict[str, Any], settings.kafka_consumer_config()))
        )

    def subscribe(self, topic: str) -> None:
        try:
            self._consumer.subscribe(
                [topic],
                on_assign=self._on_assign,
                on_revoke=self._on_revoke,
            )
        except Exception as exc:
            raise ConsumeError("Kafka subscription failed") from exc

    def poll(self, timeout: float) -> ConsumedRecord | None:
        try:
            message = self._consumer.poll(timeout)
        except Exception as exc:
            raise ConsumeError("Kafka poll failed") from exc

        if message is None:
            return None

        error = message.error()
        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:
                LOGGER.debug(
                    "Kafka partition reached end",
                    extra={
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset(),
                    },
                )
                return None
            raise ConsumeError(f"Kafka consumer error: {error}")

        topic = message.topic()
        partition = message.partition()
        offset = message.offset()
        if topic is None or partition is None or offset is None:
            raise ConsumeError("Kafka message has incomplete source metadata")

        raw_headers = (
            cast(
                Sequence[tuple[str, bytes | None]] | None,
                message.headers(),
            )
            or ()
        )
        headers: list[tuple[str, bytes | None]] = []
        for name, value in raw_headers:
            if not isinstance(name, str):
                raise ConsumeError("Kafka message contains a non-string header name")
            headers.append((name, _optional_bytes(value, "header")))

        value = _optional_bytes(message.value(), "value")
        return ConsumedRecord(
            topic=topic,
            partition=partition,
            offset=offset,
            key=_optional_bytes(message.key(), "key"),
            value=value if value is not None else b"",
            headers=tuple(headers),
            native_message=message,
        )

    def commit(self, record: ConsumedRecord) -> None:
        try:
            partitions = self._consumer.commit(
                message=cast(Message, record.native_message),
                asynchronous=False,
            )
        except Exception as exc:
            raise ConsumeError("Kafka offset commit failed") from exc

        if partitions is None:
            raise ConsumeError("Synchronous Kafka commit returned no result")
        failed = [partition for partition in partitions if partition.error is not None]
        if failed:
            details = ", ".join(_partition_name(partition) for partition in failed)
            raise ConsumeError(f"Kafka offset commit failed for {details}")

    def close(self) -> None:
        try:
            self._consumer.close()
        except Exception as exc:
            raise ConsumeError("Kafka consumer close failed") from exc

    @staticmethod
    def _on_assign(_consumer: Consumer, partitions: list[TopicPartition]) -> None:
        LOGGER.info(
            "Kafka partitions assigned",
            extra={
                "assigned_partitions": [_partition_fields(partition) for partition in partitions]
            },
        )

    @staticmethod
    def _on_revoke(_consumer: Consumer, partitions: list[TopicPartition]) -> None:
        LOGGER.info(
            "Kafka partitions revoked",
            extra={
                "revoked_partitions": [_partition_fields(partition) for partition in partitions]
            },
        )


class KafkaPublisher:
    """Publish a complete batch and wait for every final delivery report."""

    def __init__(
        self,
        settings: Settings,
        metrics: Metrics,
        native_producer: Producer | None = None,
    ) -> None:
        self._producer = (
            native_producer
            if native_producer is not None
            else Producer(cast(dict[str, Any], settings.kafka_producer_config()))
        )
        self._metrics = metrics
        self._publish_timeout_seconds = settings.kafka_publish_timeout_seconds
        self._shutdown_flush_timeout_seconds = settings.kafka_shutdown_flush_timeout_seconds

    def publish_and_wait(self, records: Sequence[OutgoingRecord]) -> None:
        if not records:
            return

        try:
            prepared = tuple(_prepare_record(record) for record in records)
        except PublishError:
            self._count_publish_errors(record.topic for record in records)
            raise
        deadline = time.monotonic() + self._publish_timeout_seconds
        acknowledged = 0
        acknowledged_by_topic: Counter[str] = Counter()
        delivery_error_count = 0

        def count_unacknowledged_errors() -> None:
            expected = Counter(record.topic for record in prepared)
            self._count_publish_errors(
                topic
                for topic, count in (expected - acknowledged_by_topic).items()
                for _ in range(count)
            )

        def delivery_callback(
            topic: str,
        ) -> Callable[[KafkaError | None, Message], None]:
            def callback(error: KafkaError | None, _message: Message) -> None:
                nonlocal acknowledged, delivery_error_count
                acknowledged += 1
                acknowledged_by_topic[topic] += 1
                outcome = "success" if error is None else "error"
                self._metrics.kafka_publish.labels(topic=topic, outcome=outcome).inc()
                if error is not None:
                    delivery_error_count += 1

            return callback

        for record in prepared:
            callback = delivery_callback(record.topic)
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    count_unacknowledged_errors()
                    raise PublishError("Kafka producer queue did not accept all records in time")
                try:
                    self._producer.produce(
                        record.topic,
                        key=record.key,
                        value=record.value,
                        headers=record.headers,
                        on_delivery=callback,
                    )
                    break
                except BufferError:
                    try:
                        self._producer.poll(min(0.1, remaining))
                    except Exception as exc:
                        count_unacknowledged_errors()
                        raise PublishError("Kafka producer poll failed") from exc
                except Exception as exc:
                    count_unacknowledged_errors()
                    raise PublishError("Kafka producer could not enqueue a record") from exc

        remaining = max(0.0, deadline - time.monotonic())
        try:
            queued = self._producer.flush(remaining)
        except Exception as exc:
            count_unacknowledged_errors()
            raise PublishError("Kafka producer flush failed") from exc

        if queued != 0 or acknowledged != len(prepared):
            count_unacknowledged_errors()
            raise PublishError("Kafka producer timed out waiting for all delivery acknowledgements")
        if delivery_error_count:
            raise PublishError(
                f"Kafka delivery failed for {delivery_error_count} outgoing record(s)"
            )

    def close(self) -> None:
        try:
            queued = self._producer.flush(self._shutdown_flush_timeout_seconds)
        except Exception as exc:
            LOGGER.exception("Kafka producer shutdown flush failed")
            raise PublishError("Kafka producer shutdown flush failed") from exc
        if queued:
            LOGGER.error(
                "Kafka producer shutdown flush timed out",
                extra={"remaining_messages": queued},
            )
            raise PublishError(f"Kafka producer shutdown left {queued} queued record(s)")

    def _count_publish_errors(self, topics: Iterable[str]) -> None:
        for topic, count in Counter(topics).items():
            self._metrics.kafka_publish.labels(topic=topic, outcome="error").inc(count)


def _prepare_record(record: OutgoingRecord) -> _PreparedRecord:
    try:
        value = _json_bytes(record.value)
    except Exception as exc:
        raise PublishError("Outgoing Kafka value is not JSON serializable") from exc

    if record.key is None:
        key = None
    elif isinstance(record.key, bytes):
        key = record.key
    elif isinstance(record.key, str):
        key = record.key.encode("utf-8")
    else:
        raise PublishError("Outgoing Kafka key must be str, bytes, or None")

    return _PreparedRecord(
        topic=record.topic,
        key=key,
        value=value,
        headers=[(name, value) for name, value in record.headers],
    )


def _json_bytes(value: BaseModel | Mapping[str, object]) -> bytes:
    payload: object = value.model_dump(mode="json") if isinstance(value, BaseModel) else dict(value)
    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _optional_bytes(value: object, field_name: str) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    raise ConsumeError(f"Kafka message {field_name} is not bytes")


def _partition_fields(partition: TopicPartition) -> dict[str, object]:
    return {"topic": partition.topic, "partition": partition.partition}


def _partition_name(partition: TopicPartition) -> str:
    return f"{partition.topic}[{partition.partition}]"
