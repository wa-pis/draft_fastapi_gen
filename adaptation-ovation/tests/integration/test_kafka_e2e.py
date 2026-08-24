from __future__ import annotations

import json
import time
from collections.abc import Mapping
from threading import Event, Thread
from typing import Any
from uuid import uuid4, uuid5

import docker
import pytest
from confluent_kafka import Consumer, KafkaError, Message, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from prometheus_client import CollectorRegistry
from testcontainers.community.kafka import KafkaContainer

from calculation_worker.application.calculation_requested_handler import (
    CalculationRequestedMessageHandler,
)
from calculation_worker.application.consumer_loop import ConsumerLoop
from calculation_worker.application.event_factory import EVENT_ID_NAMESPACE, EventFactory
from calculation_worker.application.message_processor import MessageProcessor
from calculation_worker.application.registries import MessageHandlerRegistry
from calculation_worker.calculations.example import ExampleCalculation
from calculation_worker.calculations.registry import CalculationRegistry
from calculation_worker.infrastructure.kafka import KafkaConsumerAdapter, KafkaPublisher
from calculation_worker.infrastructure.observability import Metrics
from calculation_worker.settings import Settings

pytestmark = pytest.mark.integration


class _StaticUpstream:
    def get_data(self, _calc_id: str) -> Mapping[str, Any]:
        return {"one": 1, "two": 2}


@pytest.fixture
def kafka() -> Any:
    try:
        client = docker.from_env()
        client.ping()
        client.close()
    except Exception:
        pytest.skip("Docker is not available")
    with KafkaContainer(image="confluentinc/cp-kafka:7.6.0") as container:
        yield container


def test_multiple_messages_and_committed_restart(kafka: KafkaContainer) -> None:
    bootstrap_servers = kafka.get_bootstrap_server()
    suffix = uuid4().hex
    topic = f"INTEGRATIONS-{suffix}"
    dlq_topic = f"INTEGRATIONS-DLQ-{suffix}"
    group_id = f"calculation-worker-{suffix}"
    _create_topics(bootstrap_servers, topic, dlq_topic)

    settings = _settings(bootstrap_servers, topic, dlq_topic, group_id, f"worker-1-{suffix}")
    stop = Event()
    errors: list[BaseException] = []
    worker = Thread(target=_run_worker, args=(settings, stop, errors), daemon=True)
    worker.start()

    request_ids = [f"request-{index}-{suffix}" for index in range(3)]
    input_producer = Producer({"bootstrap.servers": bootstrap_servers})
    source_offsets: dict[int, int] = {}
    input_delivery_errors: list[KafkaError] = []

    def record_input_delivery(error: KafkaError | None, message: Message) -> None:
        if error is not None:
            input_delivery_errors.append(error)
            return
        source_offsets[message.partition()] = max(
            source_offsets.get(message.partition(), -1), message.offset()
        )

    for request_id in request_ids:
        input_producer.produce(
            topic,
            key=request_id.encode(),
            value=json.dumps(
                {
                    "event_type": "calculation.requested",
                    "schema_version": 1,
                    "request_id": request_id,
                    "calc_id": f"calc-{request_id}",
                    "calc_process": "example",
                }
            ).encode(),
            on_delivery=record_input_delivery,
        )
    assert input_producer.flush(10) == 0
    assert input_delivery_errors == []

    observer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"observer-{suffix}",
            "auto.offset.reset": "earliest",
        }
    )
    observer.subscribe([topic])
    completed = _collect_completed(observer, len(request_ids), timeout_seconds=30)
    stop.set()
    worker.join(timeout=15)

    assert not worker.is_alive()
    assert errors == []
    _assert_group_committed_past_inputs(
        bootstrap_servers,
        topic,
        group_id,
        source_offsets,
    )
    assert set(completed) == set(request_ids)
    for request_id, (key, event_id) in completed.items():
        assert key == request_id.encode()
        assert event_id == str(
            uuid5(
                EVENT_ID_NAMESPACE,
                f"{request_id}:example:calculation.completed",
            )
        )

    restart_settings = _settings(
        bootstrap_servers,
        topic,
        dlq_topic,
        group_id,
        f"worker-2-{suffix}",
    )
    restart_stop = Event()
    restart_errors: list[BaseException] = []
    restarted = Thread(
        target=_run_worker,
        args=(restart_settings, restart_stop, restart_errors),
        daemon=True,
    )
    restarted.start()
    sentinel_ids = [f"sentinel-{partition}-{suffix}" for partition in range(2)]
    for partition, request_id in enumerate(sentinel_ids):
        input_producer.produce(
            topic,
            partition=partition,
            key=request_id.encode(),
            value=json.dumps(
                {
                    "event_type": "calculation.requested",
                    "schema_version": 1,
                    "request_id": request_id,
                    "calc_id": f"calc-{request_id}",
                    "calc_process": "example",
                }
            ).encode(),
        )
    assert input_producer.flush(10) == 0
    after_restart = _collect_completed(observer, len(sentinel_ids), timeout_seconds=30)
    restart_stop.set()
    restarted.join(timeout=15)
    observer.close()

    assert not restarted.is_alive()
    assert restart_errors == []
    assert set(after_restart) == set(sentinel_ids)


def _settings(
    bootstrap_servers: str,
    topic: str,
    dlq_topic: str,
    group_id: str,
    client_id: str,
) -> Settings:
    return Settings(
        _env_file=None,
        UPSTREAM_API_BASE_URL="http://unused.test",
        UPSTREAM_API_TOKEN="integration-test-token",
        KAFKA_BOOTSTRAP_SERVERS=bootstrap_servers,
        KAFKA_TOPIC=topic,
        KAFKA_DLQ_TOPIC=dlq_topic,
        KAFKA_GROUP_ID=group_id,
        KAFKA_CLIENT_ID=client_id,
        KAFKA_MAX_POLL_INTERVAL_MS=30_000,
        KAFKA_SESSION_TIMEOUT_MS=6_000,
        KAFKA_DELIVERY_TIMEOUT_MS=5_000,
        KAFKA_REQUEST_TIMEOUT_MS=2_000,
        KAFKA_PUBLISH_TIMEOUT_SECONDS=6,
        KAFKA_POLL_TIMEOUT_SECONDS=0.2,
        UPSTREAM_CONNECT_TIMEOUT_SECONDS=0.1,
        UPSTREAM_READ_TIMEOUT_SECONDS=0.1,
        UPSTREAM_WRITE_TIMEOUT_SECONDS=0.1,
        UPSTREAM_POOL_TIMEOUT_SECONDS=0.1,
        UPSTREAM_MAX_ATTEMPTS=1,
    )


def _run_worker(settings: Settings, stop: Event, errors: list[BaseException]) -> None:
    consumer: KafkaConsumerAdapter | None = None
    publisher: KafkaPublisher | None = None
    try:
        metrics = Metrics(CollectorRegistry())
        calculations = CalculationRegistry()
        calculations.register(ExampleCalculation(_StaticUpstream()))
        factory = EventFactory(
            settings.kafka_topic, settings.kafka_dlq_topic, settings.service_name
        )
        messages = MessageHandlerRegistry()
        messages.register(CalculationRequestedMessageHandler(calculations, factory, metrics))
        processor = MessageProcessor(messages, factory, metrics)
        consumer = KafkaConsumerAdapter(settings)
        publisher = KafkaPublisher(settings, metrics)
        loop = ConsumerLoop(consumer, processor, publisher, stop, settings, metrics)
        loop.run()
    except BaseException as error:
        errors.append(error)
    finally:
        if consumer is not None:
            consumer.close()
        if publisher is not None:
            publisher.close()


def _create_topics(bootstrap_servers: str, topic: str, dlq_topic: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    futures = admin.create_topics(
        [
            NewTopic(topic, num_partitions=2, replication_factor=1),
            NewTopic(dlq_topic, num_partitions=2, replication_factor=1),
        ]
    )
    for future in futures.values():
        future.result(timeout=15)


def _assert_group_committed_past_inputs(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    source_offsets: dict[int, int],
) -> None:
    inspector = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
        }
    )
    try:
        committed = inspector.committed(
            [TopicPartition(topic, partition) for partition in range(2)],
            timeout=10,
        )
    finally:
        inspector.close()
    committed_offsets = {partition.partition: partition.offset for partition in committed}
    assert all(
        committed_offsets[partition] > source_offset
        for partition, source_offset in source_offsets.items()
    )


def _collect_completed(
    consumer: Consumer,
    expected: int,
    timeout_seconds: float,
) -> dict[str, tuple[bytes | None, str]]:
    completed: dict[str, tuple[bytes | None, str]] = {}
    deadline = time.monotonic() + timeout_seconds
    while len(completed) < expected and time.monotonic() < deadline:
        message = consumer.poll(min(0.5, max(0.0, deadline - time.monotonic())))
        if message is None or message.error() is not None or message.value() is None:
            continue
        payload = json.loads(message.value())
        if payload.get("event_type") == "calculation.completed":
            completed[payload["request_id"]] = (message.key(), payload["event_id"])
    return completed
