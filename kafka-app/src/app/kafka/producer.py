import json
import logging
from typing import Any

from aiokafka import AIOKafkaProducer
from opentelemetry import propagate, trace
from opentelemetry.trace import Status, StatusCode

from app.config import KafkaSettings
from app.schemas.events import ProcessingResult

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class KafkaProducer:
    def __init__(self, settings: KafkaSettings) -> None:
        self._settings = settings
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        if not self._settings.output_topic:
            logger.info("Kafka producer disabled because output topic is not configured")
            return
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._settings.bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            key_serializer=lambda value: value.encode("utf-8") if value else None,
        )
        await self._producer.start()
        logger.info("Kafka producer started", extra={"topic": self._settings.output_topic})

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()
            logger.info("Kafka producer stopped")

    async def send_result(self, result: ProcessingResult) -> None:
        if not self._producer or not self._settings.output_topic:
            return

        with tracer.start_as_current_span("kafka.produce") as span:
            span.set_attribute("messaging.system", "kafka")
            span.set_attribute("messaging.destination.name", self._settings.output_topic)
            span.set_attribute("correlation_id", result.correlation_id or "")

            headers: dict[str, str] = {}
            propagate.inject(headers)
            kafka_headers = [(key, value.encode("utf-8")) for key, value in headers.items()]
            if result.correlation_id:
                kafka_headers.append(("correlation_id", result.correlation_id.encode("utf-8")))

            try:
                payload: dict[str, Any] = result.model_dump(mode="json")
                metadata = await self._producer.send_and_wait(
                    self._settings.output_topic,
                    value=payload,
                    key=str(result.event_id),
                    headers=kafka_headers,
                )
                logger.info(
                    "Kafka result sent",
                    extra={
                        "topic": metadata.topic,
                        "partition": metadata.partition,
                        "offset": metadata.offset,
                        "event_id": str(result.event_id),
                    },
                )
            except Exception as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                logger.exception("Kafka result send failed", extra={"event_id": str(result.event_id)})
                raise
