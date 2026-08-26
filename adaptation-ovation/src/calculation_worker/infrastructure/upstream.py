from __future__ import annotations

import logging
from collections.abc import Mapping
from time import monotonic
from typing import Any
from urllib.parse import quote

import httpx

from calculation_worker.errors import (
    InvalidUpstreamResponseError,
    UpstreamHttpError,
    UpstreamNetworkError,
    UpstreamTimeoutError,
)
from calculation_worker.infrastructure.observability import Metrics
from calculation_worker.settings import Settings

logger = logging.getLogger(__name__)


class UpstreamApiClient:
    def __init__(
        self,
        http_client: httpx.Client,
        settings: Settings,
        metrics: Metrics,
    ) -> None:
        self._http_client = http_client
        self._path_template = settings.upstream_api_path_template
        self._metrics = metrics

    def get_data(self, calc_id: str) -> Mapping[str, Any]:
        path = self._path_template.replace("{calc_id}", _encode_path_segment(calc_id))
        try:
            result = self._request_once(path)
            logger.info(
                "Upstream request completed",
                extra={"calc_id": calc_id, "upstream_attempts": 1, "outcome": "success"},
            )
            return result
        except httpx.TimeoutException as error:
            self._log_terminal_failure(calc_id, "timeout")
            raise UpstreamTimeoutError from error
        except httpx.NetworkError as error:
            self._log_terminal_failure(calc_id, "network_error")
            raise UpstreamNetworkError from error
        except UpstreamHttpError:
            self._log_terminal_failure(calc_id, "http_error")
            raise
        except InvalidUpstreamResponseError:
            self._log_terminal_failure(calc_id, "invalid_response")
            raise

    def _request_once(self, path: str) -> Mapping[str, Any]:
        started_at = monotonic()
        outcome = "error"
        try:
            response = self._http_client.get(path)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError:
                outcome = "http_error"
                raise UpstreamHttpError(response.status_code) from None
            try:
                payload = response.json()
            except ValueError:
                outcome = "invalid_response"
                raise InvalidUpstreamResponseError from None
            if not isinstance(payload, dict):
                outcome = "invalid_response"
                raise InvalidUpstreamResponseError
            outcome = "success"
            return payload
        except httpx.TimeoutException:
            outcome = "timeout"
            raise
        except httpx.NetworkError:
            outcome = "network_error"
            raise
        finally:
            self._metrics.upstream_requests.labels(outcome).inc()
            self._metrics.upstream_request_duration.observe(monotonic() - started_at)

    @staticmethod
    def _log_terminal_failure(calc_id: str, outcome: str) -> None:
        logger.warning(
            "Upstream request failed",
            extra={"calc_id": calc_id, "upstream_attempts": 1, "outcome": outcome},
        )


def create_http_client(settings: Settings) -> httpx.Client:
    base_url, token = settings.require_upstream_config()
    return httpx.Client(
        base_url=str(base_url).rstrip("/"),
        headers={"Authorization": f"Bearer {token.get_secret_value()}"},
        timeout=httpx.Timeout(
            connect=settings.upstream_connect_timeout_seconds,
            read=settings.upstream_read_timeout_seconds,
            write=settings.upstream_write_timeout_seconds,
            pool=settings.upstream_pool_timeout_seconds,
        ),
        limits=httpx.Limits(
            max_connections=settings.upstream_max_connections,
            max_keepalive_connections=settings.upstream_max_keepalive_connections,
        ),
    )


def _encode_path_segment(value: str) -> str:
    return quote(value, safe="").replace(".", "%2E")
