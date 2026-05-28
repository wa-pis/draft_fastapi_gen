import logging

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from app.schemas.events import IncomingEvent, ProcessingResult
from app.services.event_service import EventService

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class EventHandler:
    def __init__(self, event_service: EventService) -> None:
        self._event_service = event_service

    async def handle(self, event: IncomingEvent) -> ProcessingResult:
        with tracer.start_as_current_span("handler.handle_event") as span:
            span.set_attribute("message_type", event.event_type)
            try:
                result = await self._event_service.process_user_event(event)
                logger.info(
                    "Event handled successfully",
                    extra={"event_id": str(event.event_id), "event_type": event.event_type},
                )
                return result
            except Exception as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                logger.exception(
                    "Event handler failed",
                    extra={"event_id": str(event.event_id), "event_type": event.event_type},
                )
                raise
