from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from fastapi import FastAPI

from app.config.settings import Settings


def setup_tracing(settings: Settings) -> None:
    if not settings.OTLP_ENDPOINT:
        return

    resource = Resource.create({"service.name": settings.SERVICE_NAME})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=settings.OTLP_ENDPOINT)
    processor = BatchSpanProcessor(exporter)
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    AsyncPGInstrumentor().instrument()


def instrument_fastapi(app: FastAPI) -> None:
    FastAPIInstrumentor.instrument_app(app)
