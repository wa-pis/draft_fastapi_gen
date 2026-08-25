from datetime import UTC, datetime

import httpx
import pytest
import respx
from prometheus_client import CollectorRegistry

from calculation_worker.application.calculation_requested_handler import (
    CalculationRequestedMessageHandler,
)
from calculation_worker.application.event_factory import EventFactory
from calculation_worker.calculations.example import ExampleCalculation
from calculation_worker.calculations.registry import CalculationRegistry
from calculation_worker.domain.models import (
    CalculationFailed,
    DeadLetterRecord,
    HandlerResult,
    MessageContext,
)
from calculation_worker.errors import (
    InvalidUpstreamResponseError,
    UpstreamHttpError,
    UpstreamNetworkError,
    UpstreamTimeoutError,
)
from calculation_worker.infrastructure.observability import Metrics
from calculation_worker.infrastructure.upstream import UpstreamApiClient, create_http_client
from calculation_worker.settings import Settings

UPSTREAM_URL = "https://upstream.test/calculations/service-456"


class _NoopPublisher:
    def publish_and_wait(self, _records: object) -> None:
        pass


def _settings() -> Settings:
    return Settings(
        _env_file=None,
        UPSTREAM_API_BASE_URL="https://upstream.test",
        UPSTREAM_API_TOKEN="not-a-real-token",
        UPSTREAM_CONNECT_TIMEOUT_SECONDS=0.1,
        UPSTREAM_READ_TIMEOUT_SECONDS=0.1,
        UPSTREAM_WRITE_TIMEOUT_SECONDS=0.1,
        UPSTREAM_POOL_TIMEOUT_SECONDS=0.1,
        UPSTREAM_MAX_CONNECTIONS=2,
        UPSTREAM_MAX_KEEPALIVE_CONNECTIONS=1,
        UPSTREAM_MAX_ATTEMPTS=3,
        UPSTREAM_RETRY_MIN_WAIT_SECONDS=0,
        UPSTREAM_RETRY_MAX_WAIT_SECONDS=0,
        KAFKA_MAX_POLL_INTERVAL_MS=10_000,
        KAFKA_DELIVERY_TIMEOUT_MS=1_000,
        KAFKA_REQUEST_TIMEOUT_MS=500,
        KAFKA_PUBLISH_TIMEOUT_SECONDS=1,
    )


def _upstream(http_client: httpx.Client, settings: Settings) -> UpstreamApiClient:
    return UpstreamApiClient(
        http_client=http_client,
        settings=settings,
        metrics=Metrics(CollectorRegistry()),
    )


def test_timeout_is_retried_exact_number_of_attempts(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get(UPSTREAM_URL).mock(side_effect=httpx.ReadTimeout("timed out"))
    settings = _settings()

    with create_http_client(settings) as http_client, pytest.raises(UpstreamTimeoutError):
        _upstream(http_client, settings).get_data("service-456")

    assert route.call_count == 3


@pytest.mark.parametrize("status_code", [429, 500, 502, 503, 504])
def test_retryable_status_is_retried(
    respx_mock: respx.MockRouter,
    status_code: int,
) -> None:
    route = respx_mock.get(UPSTREAM_URL).mock(
        side_effect=[
            httpx.Response(status_code),
            httpx.Response(200, json={"field": "value"}),
        ]
    )
    settings = _settings()

    with create_http_client(settings) as http_client:
        result = _upstream(http_client, settings).get_data("service-456")

    assert result == {"field": "value"}
    assert route.call_count == 2


def test_network_error_is_retried_and_mapped(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get(UPSTREAM_URL).mock(side_effect=httpx.ConnectError("network unavailable"))
    settings = _settings()

    with create_http_client(settings) as http_client, pytest.raises(UpstreamNetworkError):
        _upstream(http_client, settings).get_data("service-456")

    assert route.call_count == 3


def test_terminal_timeout_creates_failed_event_and_dlq(
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get(UPSTREAM_URL).mock(side_effect=httpx.ReadTimeout("timed out"))
    settings = _settings()

    with create_http_client(settings) as http_client:
        result = _handle_request(_upstream(http_client, settings))

    failed, dead_letter = (record.value for record in result.records)
    assert isinstance(failed, CalculationFailed)
    assert failed.error.code == "UPSTREAM_TIMEOUT"
    assert isinstance(dead_letter, DeadLetterRecord)
    assert dead_letter.error.code == "UPSTREAM_TIMEOUT"


def test_http_400_is_not_retried(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get(UPSTREAM_URL).mock(return_value=httpx.Response(400))
    settings = _settings()

    with (
        create_http_client(settings) as http_client,
        pytest.raises(UpstreamHttpError) as captured,
    ):
        _upstream(http_client, settings).get_data("service-456")

    assert captured.value.status_code == 400
    assert route.call_count == 1


@pytest.mark.parametrize(
    ("calc_id", "encoded_segment"),
    [("part/child", "part%2Fchild"), (".", "%2E"), ("..", "%2E%2E")],
)
def test_calc_id_is_encoded_as_one_safe_path_segment(
    respx_mock: respx.MockRouter,
    calc_id: str,
    encoded_segment: str,
) -> None:
    route = respx_mock.get(f"https://upstream.test/calculations/{encoded_segment}").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    settings = _settings()

    with create_http_client(settings) as http_client:
        result = _upstream(http_client, settings).get_data(calc_id)

    assert result == {"ok": True}
    assert route.call_count == 1
    assert route.calls[0].request.url.raw_path.endswith(encoded_segment.encode())


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, text="{not valid json"),
        httpx.Response(200, json=["not", "an", "object"]),
    ],
    ids=["invalid-json", "json-array"],
)
def test_invalid_upstream_payload_creates_failed_event_and_dlq(
    respx_mock: respx.MockRouter,
    response: httpx.Response,
) -> None:
    route = respx_mock.get(UPSTREAM_URL).mock(return_value=response)
    settings = _settings()

    with create_http_client(settings) as http_client:
        result = _handle_request(_upstream(http_client, settings))

    assert route.call_count == 1
    assert result.outcome == "failed"
    assert len(result.records) == 2
    failed, dead_letter = (record.value for record in result.records)
    assert isinstance(failed, CalculationFailed)
    assert failed.error.code == "INVALID_UPSTREAM_RESPONSE"
    assert isinstance(dead_letter, DeadLetterRecord)
    assert dead_letter.error.code == "INVALID_UPSTREAM_RESPONSE"


def test_invalid_upstream_json_raises_explicit_error(respx_mock: respx.MockRouter) -> None:
    respx_mock.get(UPSTREAM_URL).mock(return_value=httpx.Response(200, text="not-json"))
    settings = _settings()

    with (
        create_http_client(settings) as http_client,
        pytest.raises(InvalidUpstreamResponseError),
    ):
        _upstream(http_client, settings).get_data("service-456")


def _handle_request(upstream: UpstreamApiClient) -> HandlerResult:
    calculations = CalculationRegistry()
    calculations.register(ExampleCalculation(upstream))
    handler = CalculationRequestedMessageHandler(
        calculation_registry=calculations,
        event_factory=EventFactory(
            output_topic="INTEGRATIONS",
            dlq_topic="INTEGRATIONS.DLQ",
            service_name="calculation-worker",
            clock=lambda: datetime(2026, 8, 25, 12, 0, tzinfo=UTC),
        ),
        metrics=Metrics(CollectorRegistry()),
        publisher=_NoopPublisher(),
    )
    return handler.handle(
        {
            "event_type": "calculation.requested",
            "schema_version": 1,
            "request_id": "request-123",
            "calc_id": "service-456",
            "calc_process": "example",
        },
        MessageContext(
            topic="INTEGRATIONS",
            partition=1,
            offset=42,
            key=b"request-123",
            headers=(),
            raw_value=b"original",
        ),
    )
