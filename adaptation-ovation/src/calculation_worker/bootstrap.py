from __future__ import annotations

import logging
from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import dataclass

import httpx
from dbos import DBOS, DBOSConfiguredInstance

from calculation_worker.application.calculation_requested_handler import (
    CalculationRequestedIngressHandler,
)
from calculation_worker.application.calculation_workflow import register_calculation_workflow
from calculation_worker.application.consumer_loop import ConsumerLoop, StopSignal
from calculation_worker.application.event_factory import EventFactory
from calculation_worker.application.message_processor import MessageProcessor
from calculation_worker.application.registries import MessageHandlerRegistry
from calculation_worker.calculations.example import ExampleCalculation
from calculation_worker.calculations.registry import CalculationRegistry
from calculation_worker.infrastructure.dbos_queue import DBOSCalculationQueue
from calculation_worker.infrastructure.kafka import KafkaConsumerAdapter, KafkaPublisher
from calculation_worker.infrastructure.observability import Metrics, MetricsServer, start_metrics
from calculation_worker.infrastructure.upstream import UpstreamApiClient, create_http_client
from calculation_worker.settings import Settings

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class IngressRuntime:
    consumer_loop: ConsumerLoop
    consumer: KafkaConsumerAdapter
    publisher: KafkaPublisher
    queue: DBOSCalculationQueue
    metrics_server: MetricsServer

    def run(self) -> None:
        self.consumer_loop.run()

    def close(self) -> bool:
        return _close_resources(
            (
                ("Kafka consumer", self.consumer.close),
                ("Kafka producer", self.publisher.close),
                ("DBOS client", self.queue.close),
                ("metrics server", self.metrics_server.close),
            )
        )


@dataclass(slots=True)
class WorkerRuntime:
    stop_signal: StopSignal
    publisher: KafkaPublisher
    http_client: httpx.Client
    metrics_server: MetricsServer
    workflow: DBOSConfiguredInstance
    shutdown_grace_seconds: int

    def run(self) -> None:
        while not self.stop_signal.wait(1.0):
            pass

    def close(self) -> bool:
        successful = True
        try:
            DBOS.destroy(workflow_completion_timeout_sec=self.shutdown_grace_seconds)
        except Exception:
            successful = False
            logger.exception("DBOS shutdown failed", extra={"outcome": "DBOS runtime"})
        return (
            _close_resources(
                (
                    ("Kafka producer", self.publisher.close),
                    ("HTTP client", self.http_client.close),
                    ("metrics server", self.metrics_server.close),
                )
            )
            and successful
        )


Runtime = IngressRuntime | WorkerRuntime


def build_ingress_runtime(settings: Settings, stop_signal: StopSignal) -> IngressRuntime:
    metrics = Metrics()
    with ExitStack() as failed_build_cleanup:
        queue = DBOSCalculationQueue(settings)
        failed_build_cleanup.callback(_close_after_failed_build, "DBOS client", queue.close)
        consumer = KafkaConsumerAdapter(settings)
        failed_build_cleanup.callback(_close_after_failed_build, "Kafka consumer", consumer.close)
        publisher = KafkaPublisher(settings, metrics)
        failed_build_cleanup.callback(_close_after_failed_build, "Kafka producer", publisher.close)

        event_factory = _event_factory(settings)
        message_registry = MessageHandlerRegistry()
        message_registry.register(CalculationRequestedIngressHandler(queue, event_factory, metrics))
        processor = MessageProcessor(message_registry, event_factory, metrics)
        loop = ConsumerLoop(consumer, processor, publisher, stop_signal, settings, metrics)
        metrics_server = start_metrics(metrics, settings.metrics_port)
        failed_build_cleanup.callback(
            _close_after_failed_build, "metrics server", metrics_server.close
        )
        runtime = IngressRuntime(loop, consumer, publisher, queue, metrics_server)
        failed_build_cleanup.pop_all()
        return runtime


def build_worker_runtime(settings: Settings, stop_signal: StopSignal) -> WorkerRuntime:
    executor_id = settings.require_worker_executor_id()
    metrics = Metrics()
    with ExitStack() as failed_build_cleanup:
        http_client = create_http_client(settings)
        failed_build_cleanup.callback(_close_after_failed_build, "HTTP client", http_client.close)
        publisher = KafkaPublisher(settings, metrics)
        failed_build_cleanup.callback(_close_after_failed_build, "Kafka producer", publisher.close)

        registry = CalculationRegistry()
        registry.register(ExampleCalculation(UpstreamApiClient(http_client, settings, metrics)))
        workflow = register_calculation_workflow(
            calculation_registry=registry,
            event_factory=_event_factory(settings),
            metrics=metrics,
            publisher=publisher,
            worker_concurrency=settings.dbos_worker_concurrency,
            max_recovery_attempts=settings.dbos_max_recovery_attempts,
        )
        DBOS(
            config={
                "name": settings.service_name,
                "system_database_url": settings.dbos_system_database_url.get_secret_value(),
                "dbos_system_schema": settings.dbos_system_schema,
                "application_version": settings.dbos_application_version,
                "executor_id": executor_id,
                "run_migrations": False,
                "max_executor_threads": settings.dbos_worker_concurrency,
                "enable_otlp": False,
            }
        )
        failed_build_cleanup.callback(_close_after_failed_build, "DBOS runtime", DBOS.destroy)
        DBOS.launch()

        metrics_server = start_metrics(metrics, settings.metrics_port)
        failed_build_cleanup.callback(
            _close_after_failed_build, "metrics server", metrics_server.close
        )
        runtime = WorkerRuntime(
            stop_signal,
            publisher,
            http_client,
            metrics_server,
            workflow,
            settings.dbos_shutdown_grace_seconds,
        )
        failed_build_cleanup.pop_all()
        return runtime


def _event_factory(settings: Settings) -> EventFactory:
    return EventFactory(settings.kafka_topic, settings.service_name)


def _close_resources(resources: tuple[tuple[str, Callable[[], object]], ...]) -> bool:
    successful = True
    for resource_name, close in resources:
        try:
            close()
        except Exception:
            successful = False
            logger.exception("Resource shutdown failed", extra={"outcome": resource_name})
    return successful


def _close_after_failed_build(resource_name: str, close: Callable[[], object]) -> None:
    try:
        close()
    except Exception:
        logger.exception(
            "Resource cleanup after failed startup failed", extra={"outcome": resource_name}
        )
