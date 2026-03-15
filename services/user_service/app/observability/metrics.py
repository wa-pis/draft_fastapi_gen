from prometheus_client import Counter, Histogram, Gauge
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from fastapi import APIRouter, Response


REQUEST_LATENCY = Histogram(
    "http_request_latency_seconds",
    "HTTP request latency",
    ["method", "path", "status"],
)
ERROR_COUNT = Counter(
    "http_errors_total",
    "HTTP error count",
    ["method", "path", "status"],
)
WORKER_THROUGHPUT = Counter(
    "worker_messages_processed_total",
    "Number of messages processed by workers",
    ["worker"],
)
DB_QUERY_LATENCY = Histogram(
    "db_query_latency_seconds",
    "Database query latency",
    ["operation"],
)
KAFKA_LAG = Gauge(
    "kafka_consumer_lag",
    "Kafka consumer lag",
    ["group", "topic", "partition"],
)


router = APIRouter()


@router.get("/metrics")
async def metrics() -> Response:
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)


def observe_request(method: str, path: str, status: int, duration: float) -> None:
    REQUEST_LATENCY.labels(method=method, path=path, status=status).observe(duration)
    if status >= 400:
        ERROR_COUNT.labels(method=method, path=path, status=status).inc()

