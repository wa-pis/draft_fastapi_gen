from __future__ import annotations

import logging
from collections.abc import Mapping
from functools import partial
from time import monotonic
from typing import Any
from urllib.parse import quote

import httpx
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_random_exponential,
)

from calculation_worker.errors import (
    InvalidUpstreamResponseError,
    UpstreamHttpError,
    UpstreamNetworkError,
    UpstreamTimeoutError,
)
from calculation_worker.infrastructure.observability import Metrics
from calculation_worker.settings import Settings

logger = logging.getLogger(__name__)


class _RetryableStatusError(Exception):
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(status_code)


class UpstreamApiClient:
    def __init__(
        self,
        http_client: httpx.Client,
        settings: Settings,
        metrics: Metrics,
    ) -> None:
        self._http_client = http_client
        self._path_template = settings.upstream_api_path_template
        self._retryable_status_codes = settings.upstream_retryable_status_codes
        self._max_attempts = settings.upstream_max_attempts
        self._retry_min_wait = settings.upstream_retry_min_wait_seconds
        self._retry_max_wait = settings.upstream_retry_max_wait_seconds
        self._metrics = metrics

    def get_data(self, calc_id: str) -> Mapping[str, Any]:
        path = self._path_template.replace("{calc_id}", _encode_path_segment(calc_id))
        attempts = 0
        retrying = Retrying(
            stop=stop_after_attempt(self._max_attempts),
            wait=wait_random_exponential(
                multiplier=self._retry_min_wait,
                min=self._retry_min_wait,
                max=self._retry_max_wait,
            ),
            retry=retry_if_exception(_is_retryable),
            before_sleep=partial(self._log_retry, calc_id=calc_id),
            reraise=True,
        )
        try:
            for attempt in retrying:
                with attempt:
                    attempts = attempt.retry_state.attempt_number
                    result = self._request_once(path)
                    logger.info(
                        "Upstream request completed",
                        extra={
                            "calc_id": calc_id,
                            "upstream_attempts": attempts,
                            "outcome": "success",
                        },
                    )
                    return result
        except httpx.TimeoutException as error:
            self._log_terminal_failure(calc_id, attempts, "timeout")
            raise UpstreamTimeoutError from error
        except httpx.NetworkError as error:
            self._log_terminal_failure(calc_id, attempts, "network_error")
            raise UpstreamNetworkError from error
        except _RetryableStatusError as error:
            self._log_terminal_failure(calc_id, attempts, "http_error")
            raise UpstreamHttpError(error.status_code) from None
        except UpstreamHttpError:
            self._log_terminal_failure(calc_id, attempts, "http_error")
            raise
        except InvalidUpstreamResponseError:
            self._log_terminal_failure(calc_id, attempts, "invalid_response")
            raise
        raise RuntimeError("Retry loop completed without a result")

    def _request_once(self, path: str) -> Mapping[str, Any]:
        started_at = monotonic()
        outcome = "error"
        try:
            response = self._http_client.get(path)
            if response.status_code in self._retryable_status_codes:
                outcome = "retryable_status"
                raise _RetryableStatusError(response.status_code)
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
    def _log_retry(retry_state: RetryCallState, *, calc_id: str) -> None:
        logger.warning(
            "Retrying upstream request",
            extra={
                "calc_id": calc_id,
                "upstream_attempts": retry_state.attempt_number,
                "outcome": "retry",
            },
        )

    @staticmethod
    def _log_terminal_failure(calc_id: str, attempts: int, outcome: str) -> None:
        logger.warning(
            "Upstream request failed",
            extra={
                "calc_id": calc_id,
                "upstream_attempts": attempts,
                "outcome": outcome,
            },
        )


def create_http_client(settings: Settings) -> httpx.Client:
    return httpx.Client(
        base_url=str(settings.upstream_api_base_url).rstrip("/"),
        headers={"Authorization": f"Bearer {settings.upstream_api_token.get_secret_value()}"},
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


def _is_retryable(error: BaseException) -> bool:
    return isinstance(
        error,
        (httpx.TimeoutException, httpx.NetworkError, _RetryableStatusError),
    )


def _encode_path_segment(value: str) -> str:
    return quote(value, safe="").replace(".", "%2E")
