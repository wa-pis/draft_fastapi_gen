from __future__ import annotations

import httpx
import pytest
import respx
from prometheus_client import CollectorRegistry

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


def _settings() -> Settings:
    return Settings(
        _env_file=None,
        DBOS_SYSTEM_DATABASE_URL="postgresql://test:test@localhost/test",
        UPSTREAM_API_BASE_URL="https://upstream.test",
        UPSTREAM_API_TOKEN="not-a-real-token",
        UPSTREAM_CONNECT_TIMEOUT_SECONDS=0.1,
        UPSTREAM_READ_TIMEOUT_SECONDS=0.1,
        UPSTREAM_WRITE_TIMEOUT_SECONDS=0.1,
        UPSTREAM_POOL_TIMEOUT_SECONDS=0.1,
        UPSTREAM_MAX_CONNECTIONS=2,
        UPSTREAM_MAX_KEEPALIVE_CONNECTIONS=1,
    )


def _upstream(http_client: httpx.Client, settings: Settings) -> UpstreamApiClient:
    return UpstreamApiClient(http_client, settings, Metrics(CollectorRegistry()))


def test_timeout_is_attempted_once(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get(UPSTREAM_URL).mock(side_effect=httpx.ReadTimeout("timed out"))
    settings = _settings()

    with create_http_client(settings) as client, pytest.raises(UpstreamTimeoutError):
        _upstream(client, settings).get_data("service-456")

    assert route.call_count == 1


def test_network_error_is_attempted_once(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get(UPSTREAM_URL).mock(side_effect=httpx.ConnectError("unavailable"))
    settings = _settings()

    with create_http_client(settings) as client, pytest.raises(UpstreamNetworkError):
        _upstream(client, settings).get_data("service-456")

    assert route.call_count == 1


@pytest.mark.parametrize("status_code", [400, 429, 500, 503])
def test_http_error_is_never_retried(
    respx_mock: respx.MockRouter,
    status_code: int,
) -> None:
    route = respx_mock.get(UPSTREAM_URL).mock(return_value=httpx.Response(status_code))
    settings = _settings()

    with create_http_client(settings) as client, pytest.raises(UpstreamHttpError) as captured:
        _upstream(client, settings).get_data("service-456")

    assert captured.value.status_code == status_code
    assert route.call_count == 1


def test_successful_object_is_returned(respx_mock: respx.MockRouter) -> None:
    respx_mock.get(UPSTREAM_URL).mock(return_value=httpx.Response(200, json={"field": 1}))
    settings = _settings()

    with create_http_client(settings) as client:
        result = _upstream(client, settings).get_data("service-456")

    assert result == {"field": 1}


@pytest.mark.parametrize(
    "response",
    [httpx.Response(200, text="not-json"), httpx.Response(200, json=["not", "object"])],
)
def test_invalid_payload_is_terminal(
    respx_mock: respx.MockRouter,
    response: httpx.Response,
) -> None:
    respx_mock.get(UPSTREAM_URL).mock(return_value=response)
    settings = _settings()

    with create_http_client(settings) as client, pytest.raises(InvalidUpstreamResponseError):
        _upstream(client, settings).get_data("service-456")


@pytest.mark.parametrize(
    ("calc_id", "encoded"),
    [("part/child", "part%2Fchild"), (".", "%2E"), ("..", "%2E%2E")],
)
def test_calc_id_is_one_encoded_path_segment(
    respx_mock: respx.MockRouter,
    calc_id: str,
    encoded: str,
) -> None:
    route = respx_mock.get(f"https://upstream.test/calculations/{encoded}").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    settings = _settings()

    with create_http_client(settings) as client:
        _upstream(client, settings).get_data(calc_id)

    assert route.call_count == 1
