from __future__ import annotations

import json
import logging
from dataclasses import replace
from time import monotonic
from typing import Any, NoReturn

from pydantic import ValidationError

from calculation_worker.application.contracts import MessageHandler
from calculation_worker.application.event_factory import EventFactory, Failure
from calculation_worker.application.registries import MessageHandlerRegistry
from calculation_worker.domain.models import (
    BaseEventEnvelope,
    HandlerResult,
    MessageContext,
)
from calculation_worker.infrastructure.observability import Metrics

logger = logging.getLogger(__name__)


class MessageProcessor:
    def __init__(
        self,
        message_registry: MessageHandlerRegistry,
        event_factory: EventFactory,
        metrics: Metrics,
    ) -> None:
        self._message_registry = message_registry
        self._event_factory = event_factory
        self._metrics = metrics

    def process(self, payload: bytes, context: MessageContext) -> HandlerResult:
        if context.raw_value is not payload:
            context = replace(context, raw_value=payload)

        parsed = self._decode(payload, context)
        if isinstance(parsed, HandlerResult):
            return parsed

        try:
            envelope = BaseEventEnvelope.model_validate(parsed)
        except ValidationError:
            raw_event_type = parsed.get("event_type")
            if not isinstance(raw_event_type, str):
                return self._invalid(
                    context,
                    Failure("INVALID_MESSAGE", "Message envelope is invalid"),
                )
            handler = self._message_registry.get(raw_event_type)
            if handler is None:
                return self._invalid(
                    context,
                    Failure("INVALID_MESSAGE", "Message envelope is invalid"),
                )
            return self._invoke_handler(handler, parsed, context, raw_event_type)

        handler = self._message_registry.get(envelope.event_type)
        if handler is None:
            self._metrics.kafka_messages_ignored.inc()
            logger.info(
                "Kafka event ignored because no handler is registered",
                extra={
                    **_context_log_fields(context),
                    "event_type": envelope.event_type,
                    "outcome": "ignored",
                },
            )
            return HandlerResult(records=(), outcome="ignored")

        return self._invoke_handler(handler, parsed, context, envelope.event_type)

    def _invoke_handler(
        self,
        handler: MessageHandler,
        parsed: dict[str, Any],
        context: MessageContext,
        event_type: str,
    ) -> HandlerResult:

        handler_name = type(handler).__name__
        started_at = monotonic()
        try:
            result = handler.handle(parsed, context)
        except Exception:
            duration = monotonic() - started_at
            self._metrics.message_handler_calls.labels(handler_name, "error").inc()
            self._metrics.message_handler_duration.labels(handler_name).observe(duration)
            logger.exception(
                "Message handler raised an unhandled error",
                extra={
                    **_context_log_fields(context),
                    "event_type": event_type,
                    "handler": handler_name,
                    "outcome": "error",
                    "processing_duration_ms": round(duration * 1000, 3),
                },
            )
            raise

        duration = monotonic() - started_at
        self._metrics.message_handler_calls.labels(handler_name, result.outcome).inc()
        self._metrics.message_handler_duration.labels(handler_name).observe(duration)
        logger.info(
            "Kafka event handled",
            extra={
                **_context_log_fields(context),
                "event_type": event_type,
                "handler": handler_name,
                "outcome": result.outcome,
                "processing_duration_ms": round(duration * 1000, 3),
            },
        )
        return result

    def _decode(self, payload: bytes, context: MessageContext) -> dict[str, Any] | HandlerResult:
        try:
            decoded = payload.decode("utf-8")
        except UnicodeDecodeError:
            return self._invalid(
                context,
                Failure("INVALID_MESSAGE", "Message is not valid UTF-8"),
            )
        try:
            parsed = json.loads(decoded, parse_constant=_reject_json_constant)
        except (json.JSONDecodeError, ValueError):
            return self._invalid(
                context,
                Failure("INVALID_MESSAGE", "Message is not valid JSON"),
            )
        if not isinstance(parsed, dict):
            return self._invalid(
                context,
                Failure("INVALID_MESSAGE", "Message JSON top level must be an object"),
            )
        return parsed

    def _invalid(self, context: MessageContext, failure: Failure) -> HandlerResult:
        self._metrics.kafka_messages_invalid.inc()
        logger.warning(
            "Invalid Kafka message will be sent to DLQ",
            extra={
                **_context_log_fields(context),
                "error_code": failure.code,
                "outcome": "invalid",
            },
        )
        return HandlerResult(
            records=(self._event_factory.dead_letter(context, failure),),
            outcome="invalid",
        )


def _reject_json_constant(_: str) -> NoReturn:
    raise ValueError("Non-standard JSON constant")


def _context_log_fields(context: MessageContext) -> dict[str, object]:
    return {
        "topic": context.topic,
        "partition": context.partition,
        "offset": context.offset,
        "kafka_key": (
            context.key.decode("utf-8", errors="replace")[:256] if context.key is not None else None
        ),
    }
