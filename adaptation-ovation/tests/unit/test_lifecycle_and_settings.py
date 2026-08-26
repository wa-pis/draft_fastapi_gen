from __future__ import annotations

import logging
import signal
import sys
from threading import Event
from typing import Any, cast

import pytest

from calculation_worker.bootstrap import IngressRuntime
from calculation_worker.errors import ConfigurationError
from calculation_worker.infrastructure.observability import JsonFormatter
from calculation_worker.main import _install_signal_handlers, _parse_mode
from calculation_worker.settings import Settings


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "_env_file": None,
        "DBOS_SYSTEM_DATABASE_URL": "postgresql://worker:database-secret@db/calculations",
        "UPSTREAM_API_BASE_URL": "https://upstream.test",
        "UPSTREAM_API_TOKEN": "super-secret-token",
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


def test_ingress_runtime_closes_every_resource_after_an_error() -> None:
    calls: list[str] = []
    runtime = IngressRuntime(
        consumer_loop=cast(Any, object()),
        consumer=cast(Any, _Resource("consumer", calls, fail=True)),
        publisher=cast(Any, _Resource("producer", calls)),
        queue=cast(Any, _Resource("dbos-client", calls)),
        metrics_server=cast(Any, _Resource("metrics", calls)),
    )

    assert runtime.close() is False
    assert calls == ["consumer", "producer", "dbos-client", "metrics"]


def test_signal_handlers_only_set_stop_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    installed: dict[signal.Signals, Any] = {}
    monkeypatch.setattr(signal, "signal", lambda sig, handler: installed.setdefault(sig, handler))
    stop = Event()

    _install_signal_handlers(stop)
    installed[signal.SIGTERM](signal.SIGTERM, None)

    assert stop.is_set()
    assert signal.SIGINT in installed


def test_two_explicit_process_modes() -> None:
    assert _parse_mode(["ingress"]) == "ingress"
    assert _parse_mode(["worker"]) == "worker"


def test_worker_requires_stable_executor_id() -> None:
    with pytest.raises(ConfigurationError, match="DBOS_EXECUTOR_ID"):
        _settings().require_worker_executor_id()

    assert _settings(DBOS_EXECUTOR_ID="worker-a").require_worker_executor_id() == "worker-a"


def test_only_postgresql_dbos_urls_are_accepted() -> None:
    with pytest.raises(ConfigurationError, match="configuration is invalid"):
        _settings(DBOS_SYSTEM_DATABASE_URL="sqlite:///local.db")


def test_dbos_schema_must_be_a_safe_identifier() -> None:
    with pytest.raises(ConfigurationError, match="configuration is invalid"):
        _settings(DBOS_SYSTEM_SCHEMA="dbos;drop schema public")


def test_secrets_are_absent_from_settings_repr_and_exception_log() -> None:
    settings = _settings()
    token = "super-secret-token"
    assert "database-secret" not in repr(settings)
    assert token not in repr(settings)

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


def test_model_validation_error_hides_secret_inputs() -> None:
    token = "validation-secret-token"
    password = "validation-secret-password"
    with pytest.raises(ConfigurationError) as captured:
        Settings.model_validate(
            {
                "DBOS_SYSTEM_DATABASE_URL": f"postgresql://worker:{password}@db/test",
                "UPSTREAM_API_BASE_URL": "https://upstream.test",
                "UPSTREAM_API_TOKEN": token,
                "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
                "KAFKA_SASL_PASSWORD": password,
            }
        )
    rendered = f"{captured.value!s} {captured.value!r}"
    assert token not in rendered
    assert password not in rendered
