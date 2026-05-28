import logging
from collections.abc import Mapping

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import (
    ALWAYS_OFF,
    ALWAYS_ON,
    ParentBased,
    TraceIdRatioBased,
)

from app.config import AppSettings, OpenTelemetrySettings

logger = logging.getLogger(__name__)

try:
    from opentelemetry.instrumentation.logging import LoggingInstrumentor
except ImportError:  # pragma: no cover - dependency is declared, but keep local imports resilient.
    LoggingInstrumentor = None


def configure_tracing(app_settings: AppSettings, otel_settings: OpenTelemetrySettings) -> None:
    if not otel_settings.enabled:
        trace.set_tracer_provider(trace.NoOpTracerProvider())
        return

    resource = Resource.create(
        {
            "service.name": otel_settings.service_name,
            "deployment.environment": app_settings.environment,
        }
    )
    provider = TracerProvider(resource=resource, sampler=_build_sampler(otel_settings))
    exporter = OTLPSpanExporter(
        endpoint=otel_settings.exporter_otlp_endpoint,
        headers=_parse_headers(otel_settings.exporter_otlp_headers),
    )
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    if LoggingInstrumentor:
        LoggingInstrumentor().instrument(set_logging_format=False)
    logger.info("OpenTelemetry tracing configured")


def shutdown_tracing() -> None:
    provider = trace.get_tracer_provider()
    shutdown = getattr(provider, "shutdown", None)
    if callable(shutdown):
        shutdown()


def _build_sampler(otel_settings: OpenTelemetrySettings):
    sampler = otel_settings.traces_sampler.lower()
    ratio = max(0.0, min(1.0, otel_settings.traces_sampler_arg))

    if sampler == "always_on":
        return ALWAYS_ON
    if sampler == "always_off":
        return ALWAYS_OFF
    if sampler == "traceidratio":
        return TraceIdRatioBased(ratio)
    if sampler == "parentbased_traceidratio":
        return ParentBased(TraceIdRatioBased(ratio))
    if sampler == "parentbased_always_on":
        return ParentBased(ALWAYS_ON)
    if sampler == "parentbased_always_off":
        return ParentBased(ALWAYS_OFF)

    logger.warning("Unknown OTEL_TRACES_SAMPLER, falling back to parentbased_traceidratio")
    return ParentBased(TraceIdRatioBased(ratio))


def _parse_headers(raw_headers: str) -> Mapping[str, str] | None:
    if not raw_headers:
        return None

    headers: dict[str, str] = {}
    for part in raw_headers.split(","):
        if not part.strip() or "=" not in part:
            continue
        key, value = part.split("=", 1)
        headers[key.strip()] = value.strip()
    return headers or None
