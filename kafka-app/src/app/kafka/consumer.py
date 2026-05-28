import json
import logging
from collections.abc import Mapping
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from opentelemetry import propagate, trace
from opentelemetry.trace import Status, StatusCode
from pydantic import ValidationError

from app.config import KafkaSettings
from app.handlers.event_handler import EventHandler
from app.kafka.producer import KafkaProducer
from app.observability.context import correlation_context
from app.schemas.events import IncomingEvent

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class KafkaConsumer:
    def __init__(
        self,
        settings: KafkaSettings,
        event_handler: EventHandler,
        producer: KafkaProducer | None = None,
    ) -> None:
        self._settings = settings
        self._event_handler = event_handler
        self._producer = producer
        self._consumer: AIOKafkaConsumer | None = None
        self._stopping = False

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            self._settings.input_topic,
            bootstrap_servers=self._settings.bootstrap_servers,
            group_id=self._settings.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await self._consumer.start()
        logger.info(
            "Kafka consumer started",
            extra={
                "topic": self._settings.input_topic,
                "group_id": self._settings.group_id,
                "bootstrap_servers": self._settings.bootstrap_servers,
            },
        )

    async def stop(self) -> None:
        self._stopping = True
        if self._consumer:
            await self._consumer.stop()
            logger.info("Kafka consumer stopped")

    async def run(self) -> None:
        if not self._consumer:
            raise RuntimeError("Kafka consumer is not started")

        try:
            async for message in self._consumer:
                if self._stopping:
                    break
                try:
                    await self._process_message(message)
                except Exception:
                    # The offset is intentionally not committed here. A retry/DLQ policy can be
                    # added around this block when the desired poison-message strategy is known.
                    continue
        except KafkaError:
            logger.exception("Kafka consumer error")
            raise

    async def _process_message(self, message: Any) -> None:
        raw_headers = _headers_to_mapping(message.headers or [])
        carrier = {key: value.decode("utf-8") for key, value in raw_headers.items()}
        parent_context = propagate.extract(carrier)

        decoded_payload: dict[str, Any] | None = None
        event: IncomingEvent | None = None
        correlation_id = _extract_header(raw_headers, "correlation_id")

        try:
            decoded_payload = json.loads(message.value.decode("utf-8"))
            if isinstance(decoded_payload, dict):
                correlation_id = decoded_payload.get("correlation_id") or correlation_id
            event = IncomingEvent.model_validate(decoded_payload)
            correlation_id = event.correlation_id or correlation_id
        except UnicodeDecodeError:
            with correlation_context(correlation_id):
                logger.exception(
                    "Kafka message decoding failed",
                    extra=_message_log_extra(message),
                )
            raise
        except json.JSONDecodeError:
            with correlation_context(correlation_id):
                logger.exception(
                    "Kafka message JSON decoding failed",
                    extra=_message_log_extra(message),
                )
            raise
        except ValidationError:
            with correlation_context(correlation_id):
                logger.exception(
                    "Kafka message validation failed",
                    extra={**_message_log_extra(message), "payload": decoded_payload},
                )
            raise

        with correlation_context(correlation_id):
            with tracer.start_as_current_span(
                "kafka.consume.process",
                context=parent_context,
            ) as span:
                _set_message_span_attributes(span, message, event, correlation_id, self._settings)
                logger.info(
                    "Kafka message received",
                    extra={
                        **_message_log_extra(message),
                        "event_id": str(event.event_id),
                        "event_type": event.event_type,
                    },
                )

                try:
                    result = await self._event_handler.handle(event)
                    if self._producer:
                        await self._producer.send_result(result)

                    await self._consumer.commit()
                    logger.info(
                        "Kafka message processed and committed",
                        extra={
                            **_message_log_extra(message),
                            "event_id": str(event.event_id),
                            "event_type": event.event_type,
                        },
                    )
                except Exception as exc:
                    span.record_exception(exc)
                    span.set_status(Status(StatusCode.ERROR, str(exc)))
                    logger.exception(
                        "Kafka message processing failed; offset was not committed",
                        extra={
                            **_message_log_extra(message),
                            "event_id": str(event.event_id),
                            "event_type": event.event_type,
                        },
                    )
                    raise


def _headers_to_mapping(headers: list[tuple[str, bytes]]) -> dict[str, bytes]:
    return {key: value for key, value in headers}


def _extract_header(headers: Mapping[str, bytes], key: str) -> str | None:
    value = headers.get(key)
    return value.decode("utf-8") if value else None


def _message_log_extra(message: Any) -> dict[str, Any]:
    return {
        "topic": message.topic,
        "partition": message.partition,
        "offset": message.offset,
        "key": message.key.decode("utf-8") if message.key else None,
        "headers": {
            key: value.decode("utf-8", errors="replace")
            for key, value in (message.headers or [])
        },
    }


def _set_message_span_attributes(
    span: trace.Span,
    message: Any,
    event: IncomingEvent,
    correlation_id: str | None,
    settings: KafkaSettings,
) -> None:
    span.set_attribute("messaging.system", "kafka")
    span.set_attribute("messaging.destination.name", message.topic)
    span.set_attribute("messaging.kafka.consumer.group", settings.group_id)
    span.set_attribute("messaging.kafka.message.offset", message.offset)
    span.set_attribute("messaging.kafka.message.partition", message.partition)
    span.set_attribute("message_type", event.event_type)
    if correlation_id:
        span.set_attribute("correlation_id", correlation_id)
