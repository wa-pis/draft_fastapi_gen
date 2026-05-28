import logging

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.schemas.events import IncomingEvent, ProcessingResult

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class BusinessLogicError(RuntimeError):
    pass


class EventService:
    @retry(
        retry=retry_if_exception_type(BusinessLogicError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.2, max=2),
        reraise=True,
    )
    async def process_user_event(self, event: IncomingEvent) -> ProcessingResult:
        with tracer.start_as_current_span("service.process_user_event") as span:
            span.set_attribute("event.id", str(event.event_id))
            span.set_attribute("event.type", event.event_type)
            try:
                if event.event_type != "user.created":
                    raise BusinessLogicError(f"Unsupported event_type: {event.event_type}")

                logger.info(
                    "User event processed",
                    extra={"event_id": str(event.event_id), "user_id": event.payload.user_id},
                )
                return ProcessingResult(
                    event_id=event.event_id,
                    event_type=event.event_type,
                    status="processed",
                    correlation_id=event.correlation_id,
                )
            except Exception as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                raise
