from __future__ import annotations

import logging
import signal
import sys
from threading import Event
from typing import Any, cast

import pytest

import calculation_worker.bootstrap as bootstrap_module
from calculation_worker.bootstrap import Runtime
from calculation_worker.errors import ConfigurationError
from calculation_worker.infrastructure.observability import JsonFormatter
from calculation_worker.main import _install_signal_handlers
from calculation_worker.settings import Settings


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "_env_file": None,
        "UPSTREAM_API_BASE_URL": "https://upstream.test",
        "UPSTREAM_API_TOKEN": "super-secret-token",
        "UPSTREAM_CONNECT_TIMEOUT_SECONDS": 0.1,
        "UPSTREAM_READ_TIMEOUT_SECONDS": 0.1,
        "UPSTREAM_WRITE_TIMEOUT_SECONDS": 0.1,
        "UPSTREAM_POOL_TIMEOUT_SECONDS": 0.1,
        "UPSTREAM_MAX_ATTEMPTS": 1,
        "KAFKA_MAX_POLL_INTERVAL_MS": 10_000,
        "KAFKA_DELIVERY_TIMEOUT_MS": 1_000,
        "KAFKA_REQUEST_TIMEOUT_MS": 500,
        "KAFKA_PUBLISH_TIMEOUT_SECONDS": 1,
    }
    values.update(overrides)
    return Settings(**values)


class _Resource:
    def __init__(self, name: str, calls: list[str], fail: bool = False) -> None:
        self.name = name
        self.calls = calls
        self.fail = fail

    def close(self) -> None:
        self.calls.append(self.name)
        if self.fail:
            raise RuntimeError(f"{self.name}-secret")


class _Loop:
    def run(self) -> None: ...


def test_runtime_closes_every_resource_in_order() -> None:
    calls: list[str] = []
    runtime = Runtime(
        consumer_loop=cast(Any, _Loop()),
        consumer=cast(Any, _Resource("consumer", calls)),
        publisher=cast(Any, _Resource("producer", calls)),
        http_client=cast(Any, _Resource("http", calls)),
        metrics_server=cast(Any, _Resource("metrics", calls)),
    )

    assert runtime.close() is True
    assert calls == ["consumer", "producer", "http", "metrics"]


def test_runtime_continues_cleanup_after_close_error() -> None:
    calls: list[str] = []
    runtime = Runtime(
        consumer_loop=cast(Any, _Loop()),
        consumer=cast(Any, _Resource("consumer", calls, fail=True)),
        publisher=cast(Any, _Resource("producer", calls)),
        http_client=cast(Any, _Resource("http", calls)),
        metrics_server=cast(Any, _Resource("metrics", calls)),
    )

    assert runtime.close() is False
    assert calls == ["consumer", "producer", "http", "metrics"]


def test_failed_runtime_build_closes_every_created_resource(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    http_client = _Resource("http", calls)
    consumer = _Resource("consumer", calls)
    publisher = _Resource("producer", calls)
    monkeypatch.setattr(bootstrap_module, "create_http_client", lambda _settings: http_client)
    monkeypatch.setattr(bootstrap_module, "KafkaConsumerAdapter", lambda _settings: consumer)
    monkeypatch.setattr(
        bootstrap_module,
        "KafkaPublisher",
        lambda _settings, _metrics: publisher,
    )

    def fail_metrics_start(_metrics: object, _port: int) -> None:
        raise RuntimeError("metrics port unavailable")

    monkeypatch.setattr(bootstrap_module, "start_metrics", fail_metrics_start)

    with pytest.raises(RuntimeError, match="metrics port unavailable"):
        bootstrap_module.build_runtime(_settings(), Event())

    assert calls == ["producer", "consumer", "http"]


def test_signal_handlers_only_set_stop_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    installed: dict[signal.Signals, Any] = {}
    monkeypatch.setattr(signal, "signal", lambda sig, handler: installed.setdefault(sig, handler))
    stop = Event()

    _install_signal_handlers(stop)
    installed[signal.SIGTERM](signal.SIGTERM, None)

    assert stop.is_set()
    assert signal.SIGINT in installed


def test_secrets_are_absent_from_settings_repr_and_exception_log() -> None:
    token = "super-secret-token"
    password = "super-secret-password"
    settings = _settings(
        KAFKA_SECURITY_PROTOCOL="SASL_SSL",
        KAFKA_SASL_MECHANISM="PLAIN",
        KAFKA_SASL_USERNAME="worker",
        KAFKA_SASL_PASSWORD=password,
    )

    assert token not in repr(settings)
    assert password not in repr(settings)

    formatter = JsonFormatter()
    try:
        raise RuntimeError(token)
    except RuntimeError:
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="Operation failed",
            args=(),
            exc_info=sys.exc_info(),
        )
    assert token not in formatter.format(record)


def test_invalid_retry_budget_is_rejected_before_startup() -> None:
    with pytest.raises(ConfigurationError, match="configuration is invalid"):
        _settings(
            UPSTREAM_READ_TIMEOUT_SECONDS=9,
            KAFKA_MAX_POLL_INTERVAL_MS=10_000,
        )


def test_runtime_budget_counts_started_and_terminal_publications() -> None:
    with pytest.raises(ConfigurationError, match="configuration is invalid"):
        _settings(KAFKA_MAX_POLL_INTERVAL_MS=2_000)


def test_upstream_base_url_rejects_embedded_credentials() -> None:
    password = "embedded-secret-password"
    with pytest.raises(ConfigurationError, match="configuration is invalid") as captured:
        _settings(UPSTREAM_API_BASE_URL=f"https://user:{password}@upstream.test")

    assert password not in str(captured.value)


def test_model_validation_error_hides_all_secret_inputs() -> None:
    token = "validation-secret-token"
    password = "validation-secret-password"
    with pytest.raises(ConfigurationError) as captured:
        _settings(
            UPSTREAM_API_TOKEN=token,
            KAFKA_SECURITY_PROTOCOL="SASL_SSL",
            KAFKA_SASL_PASSWORD=password,
        )

    rendered = f"{captured.value!s} {captured.value!r}"
    assert token not in rendered
    assert password not in rendered

    with pytest.raises(ConfigurationError) as model_validate_error:
        Settings.model_validate(
            {
                "UPSTREAM_API_BASE_URL": "https://upstream.test",
                "UPSTREAM_API_TOKEN": token,
                "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
                "KAFKA_SASL_PASSWORD": password,
            }
        )
    assert token not in repr(model_validate_error.value)
    assert password not in repr(model_validate_error.value)
