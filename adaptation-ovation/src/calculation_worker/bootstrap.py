from __future__ import annotations

import logging
from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import dataclass

import httpx

from calculation_worker.application.calculation_requested_handler import (
    CalculationRequestedMessageHandler,
)
from calculation_worker.application.consumer_loop import ConsumerLoop, StopSignal
from calculation_worker.application.event_factory import EventFactory
from calculation_worker.application.message_processor import MessageProcessor
from calculation_worker.application.registries import MessageHandlerRegistry
from calculation_worker.calculations.example import ExampleCalculation
from calculation_worker.calculations.registry import CalculationRegistry
from calculation_worker.infrastructure.kafka import KafkaConsumerAdapter, KafkaPublisher
from calculation_worker.infrastructure.observability import (
    Metrics,
    MetricsServer,
    start_metrics,
)
from calculation_worker.infrastructure.upstream import UpstreamApiClient, create_http_client
from calculation_worker.settings import Settings

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class Runtime:
    consumer_loop: ConsumerLoop
    consumer: KafkaConsumerAdapter
    publisher: KafkaPublisher
    http_client: httpx.Client
    metrics_server: MetricsServer

    def run(self) -> None:
        self.consumer_loop.run()

    def close(self) -> bool:
        successful = True
        resources = (
            ("Kafka consumer", self.consumer.close),
            ("Kafka producer", self.publisher.close),
            ("HTTP client", self.http_client.close),
            ("metrics server", self.metrics_server.close),
        )
        for resource_name, close in resources:
            try:
                close()
            except Exception:
                successful = False
                logger.exception("Resource shutdown failed", extra={"outcome": resource_name})
        return successful


def build_runtime(settings: Settings, stop_signal: StopSignal) -> Runtime:
    metrics = Metrics()
    with ExitStack() as failed_build_cleanup:
        http_client = create_http_client(settings)
        failed_build_cleanup.callback(_close_after_failed_build, "HTTP client", http_client.close)
        upstream_client = UpstreamApiClient(http_client, settings, metrics)

        calculation_registry = CalculationRegistry()
        calculation_registry.register(ExampleCalculation(upstream_client))

        event_factory = EventFactory(
            output_topic=settings.kafka_topic,
            dlq_topic=settings.kafka_dlq_topic,
            service_name=settings.service_name,
        )
        message_registry = MessageHandlerRegistry()
        message_registry.register(
            CalculationRequestedMessageHandler(
                calculation_registry=calculation_registry,
                event_factory=event_factory,
                metrics=metrics,
            )
        )

        processor = MessageProcessor(message_registry, event_factory, metrics)
        consumer = KafkaConsumerAdapter(settings)
        failed_build_cleanup.callback(_close_after_failed_build, "Kafka consumer", consumer.close)
        publisher = KafkaPublisher(settings, metrics)
        failed_build_cleanup.callback(_close_after_failed_build, "Kafka producer", publisher.close)
        consumer_loop = ConsumerLoop(
            consumer=consumer,
            processor=processor,
            publisher=publisher,
            stop_signal=stop_signal,
            settings=settings,
            metrics=metrics,
        )
        metrics_server = start_metrics(metrics, settings.metrics_port)
        failed_build_cleanup.callback(
            _close_after_failed_build, "metrics server", metrics_server.close
        )
        runtime = Runtime(
            consumer_loop=consumer_loop,
            consumer=consumer,
            publisher=publisher,
            http_client=http_client,
            metrics_server=metrics_server,
        )
        failed_build_cleanup.pop_all()
        return runtime


def _close_after_failed_build(resource_name: str, close: Callable[[], object]) -> None:
    try:
        close()
    except Exception:
        logger.exception(
            "Resource cleanup after failed startup failed",
            extra={"outcome": resource_name},
        )
