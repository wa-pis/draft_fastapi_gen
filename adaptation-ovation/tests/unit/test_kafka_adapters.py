from __future__ import annotations

import json
import logging
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any, cast

import pytest
from confluent_kafka import Consumer, KafkaError, Message, TopicPartition

import calculation_worker.infrastructure.kafka as kafka_module
from calculation_worker.application.consumer_loop import ConsumedRecord
from calculation_worker.domain.models import OutgoingRecord
from calculation_worker.errors import ConsumeError, PublishError
from calculation_worker.infrastructure.kafka import KafkaConsumerAdapter, KafkaPublisher
from calculation_worker.infrastructure.observability import Metrics
from calculation_worker.settings import Settings

DeliveryCallback = Callable[[KafkaError | None, Message], None]


class FakeNativeConsumer:
    def __init__(self) -> None:
        self.topics: list[str] = []
        self.on_assign: Callable[[Consumer, list[TopicPartition]], None] | None = None
        self.on_revoke: Callable[[Consumer, list[TopicPartition]], None] | None = None
        self.commit_result: list[Any] = []
        self.commit_message: object | None = None
        self.commit_asynchronous: bool | None = None

    def subscribe(
        self,
        topics: list[str],
        *,
        on_assign: Callable[[Consumer, list[TopicPartition]], None],
        on_revoke: Callable[[Consumer, list[TopicPartition]], None],
    ) -> None:
        self.topics = topics
        self.on_assign = on_assign
        self.on_revoke = on_revoke

    def poll(self, _timeout: float) -> None:
        return None

    def commit(self, *, message: object, asynchronous: bool) -> list[Any]:
        self.commit_message = message
        self.commit_asynchronous = asynchronous
        return self.commit_result

    def close(self) -> None:
        pass


class FakeNativeProducer:
    def __init__(
        self,
        *,
        buffer_errors: int = 0,
        always_buffer: bool = False,
        delivery_errors: list[KafkaError | None] | None = None,
        flush_result: int = 0,
    ) -> None:
        self.buffer_errors = buffer_errors
        self.always_buffer = always_buffer
        self.delivery_errors = delivery_errors or []
        self.flush_result = flush_result
        self.attempts = 0
        self.poll_timeouts: list[float] = []
        self.flush_timeouts: list[float] = []
        self.produced: list[dict[str, object]] = []
        self._callbacks: list[DeliveryCallback] = []

    def produce(
        self,
        topic: str,
        *,
        key: bytes | None,
        value: bytes,
        headers: list[tuple[str, str | bytes | None]],
        on_delivery: DeliveryCallback,
    ) -> None:
        self.attempts += 1
        if self.always_buffer or self.buffer_errors > 0:
            self.buffer_errors -= 1
            raise BufferError("queue full")
        self.produced.append({"topic": topic, "key": key, "value": value, "headers": headers})
        self._callbacks.append(on_delivery)

    def poll(self, timeout: float) -> int:
        self.poll_timeouts.append(timeout)
        return 0

    def flush(self, timeout: float) -> int:
        self.flush_timeouts.append(timeout)
        callbacks, self._callbacks = self._callbacks, []
        for index, callback in enumerate(callbacks):
            error = self.delivery_errors[index] if index < len(self.delivery_errors) else None
            callback(error, cast(Message, object()))
        return self.flush_result


def test_subscribe_registers_logging_callbacks_without_manual_assign(
    caplog: pytest.LogCaptureFixture,
) -> None:
    native = FakeNativeConsumer()
    adapter = KafkaConsumerAdapter(_settings(), cast(Consumer, native))
    adapter.subscribe("INTEGRATIONS")
    partitions = [TopicPartition("INTEGRATIONS", 2)]

    with caplog.at_level(logging.INFO):
        assert native.on_assign is not None
        assert native.on_revoke is not None
        native.on_assign(cast(Consumer, native), partitions)
        native.on_revoke(cast(Consumer, native), partitions)

    assert native.topics == ["INTEGRATIONS"]
    assert not hasattr(native, "assign")
    assert caplog.records[-2].assigned_partitions == [{"topic": "INTEGRATIONS", "partition": 2}]
    assert caplog.records[-1].revoked_partitions == [{"topic": "INTEGRATIONS", "partition": 2}]


def test_sync_commit_rejects_per_partition_error() -> None:
    native = FakeNativeConsumer()
    native.commit_result = [
        SimpleNamespace(topic="INTEGRATIONS", partition=1, error=RuntimeError("failed"))
    ]
    adapter = KafkaConsumerAdapter(_settings(), cast(Consumer, native))
    record = _consumed(native_message=object())

    with pytest.raises(ConsumeError, match=r"INTEGRATIONS\[1\]"):
        adapter.commit(record)

    assert native.commit_message is record.native_message
    assert native.commit_asynchronous is False


def test_publisher_waits_for_every_callback_and_counts_dlq_success() -> None:
    native = FakeNativeProducer()
    metrics = Metrics()
    publisher = KafkaPublisher(_settings(), metrics, cast(Any, native))
    records = (
        OutgoingRecord(
            topic="INTEGRATIONS",
            key="request-1",
            value={"status": "ok"},
            headers=(("trace", b"one"),),
        ),
        OutgoingRecord(
            topic="INTEGRATIONS.DLQ",
            key=b"request-1",
            value={"error": {"code": "INVALID_MESSAGE"}},
        ),
    )

    publisher.publish_and_wait(records)

    assert len(native.produced) == 2
    assert native.produced[0]["key"] == b"request-1"
    assert json.loads(cast(bytes, native.produced[0]["value"])) == {"status": "ok"}
    assert _counter_value(metrics.dlq_messages, "dlq_messages_total") == 1


def test_callback_error_fails_even_when_flush_queue_is_empty() -> None:
    native = FakeNativeProducer(
        delivery_errors=[None, KafkaError(KafkaError._MSG_TIMED_OUT)],
        flush_result=0,
    )
    metrics = Metrics()
    publisher = KafkaPublisher(_settings(), metrics, cast(Any, native))
    records = (
        _record(),
        OutgoingRecord(
            topic="INTEGRATIONS.DLQ",
            key="request-1",
            value={"error": "failed"},
        ),
    )

    with pytest.raises(PublishError, match="delivery failed"):
        publisher.publish_and_wait(records)

    assert len(native.produced) == 2
    assert (
        _labeled_counter_value(
            metrics.kafka_publish,
            topic="INTEGRATIONS",
            outcome="success",
        )
        == 1
    )
    assert (
        _labeled_counter_value(
            metrics.kafka_publish,
            topic="INTEGRATIONS.DLQ",
            outcome="error",
        )
        == 1
    )


def test_serialization_failure_is_counted_as_publish_error() -> None:
    metrics = Metrics()
    publisher = KafkaPublisher(_settings(), metrics, cast(Any, FakeNativeProducer()))

    with pytest.raises(PublishError, match="not JSON serializable"):
        publisher.publish_and_wait(
            (OutgoingRecord(topic="INTEGRATIONS", key="request-1", value={"bad": float("nan")}),)
        )

    assert (
        _labeled_counter_value(
            metrics.kafka_publish,
            topic="INTEGRATIONS",
            outcome="error",
        )
        == 1
    )


def test_full_local_queue_is_polled_then_retried() -> None:
    native = FakeNativeProducer(buffer_errors=1)
    publisher = KafkaPublisher(_settings(), Metrics(), cast(Any, native))

    publisher.publish_and_wait((_record(),))

    assert native.attempts == 2
    assert len(native.poll_timeouts) == 1
    assert len(native.produced) == 1


def test_queue_wait_uses_one_bounded_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    native = FakeNativeProducer(always_buffer=True)
    publisher = KafkaPublisher(
        _settings(
            KAFKA_DELIVERY_TIMEOUT_MS=1_000,
            KAFKA_REQUEST_TIMEOUT_MS=500,
            KAFKA_PUBLISH_TIMEOUT_SECONDS=1.0,
        ),
        Metrics(),
        cast(Any, native),
    )
    clock = iter((0.0, 0.0, 2.0))
    monkeypatch.setattr(kafka_module.time, "monotonic", lambda: next(clock))

    with pytest.raises(PublishError, match="did not accept"):
        publisher.publish_and_wait((_record(),))

    assert native.poll_timeouts == [0.1]
    assert native.flush_timeouts == []


def test_close_uses_bounded_flush_and_fails_if_records_remain(
    caplog: pytest.LogCaptureFixture,
) -> None:
    native = FakeNativeProducer(flush_result=3)
    publisher = KafkaPublisher(
        _settings(KAFKA_SHUTDOWN_FLUSH_TIMEOUT_SECONDS=2.5),
        Metrics(),
        cast(Any, native),
    )

    with caplog.at_level(logging.ERROR), pytest.raises(PublishError, match="3 queued"):
        publisher.close()

    assert native.flush_timeouts == [2.5]
    assert caplog.records[-1].remaining_messages == 3


def _record() -> OutgoingRecord:
    return OutgoingRecord(
        topic="INTEGRATIONS",
        key="request-1",
        value={"status": "ok"},
    )


def _consumed(*, native_message: object) -> ConsumedRecord:
    return ConsumedRecord(
        topic="INTEGRATIONS",
        partition=1,
        offset=10,
        key=b"request-1",
        value=b"{}",
        headers=(),
        native_message=native_message,
    )


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "UPSTREAM_API_BASE_URL": "http://upstream.test",
        "UPSTREAM_API_TOKEN": "secret",
    }
    values.update(overrides)
    return Settings.model_validate(values)


def _counter_value(counter: Any, sample_name: str) -> float:
    return next(
        sample.value
        for metric in counter.collect()
        for sample in metric.samples
        if sample.name == sample_name
    )


def _labeled_counter_value(counter: Any, **labels: str) -> float:
    return next(
        sample.value
        for metric in counter.collect()
        for sample in metric.samples
        if sample.labels == labels and sample.name.endswith("_total")
    )
