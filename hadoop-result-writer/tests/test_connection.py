from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from unittest.mock import Mock

import pytest

from hadoop_result_writer.config import Config, hdfs_uri, result_identifier
from hadoop_result_writer.errors import AuthenticationError
from hadoop_result_writer.kerberos import Kerberos
from hadoop_result_writer.storage import Hdfs


@pytest.fixture
def config(tmp_path):
    return Config(
        principal="worker@EXAMPLE.TEST",
        keytab="/run/secrets/worker.keytab",
        hdfs_root="hdfs://cluster/writer",
        trino_host="trino.example.test",
        trino_user="worker",
        catalog="hive",
        schema="demo",
        table="results",
        cache_dir=str(tmp_path),
        refresh_seconds=0.03,
    )


@pytest.mark.parametrize(
    "uri",
    [
        "hdfs://cluster/",
        "s3://bucket/root",
        "hdfs://u:p@cluster/a",
        "hdfs://cluster/a/../b",
        "hdfs://cluster/a?q=1",
    ],
)
def test_bad_hdfs_paths(uri):
    with pytest.raises(ValueError):
        hdfs_uri(uri)


@pytest.mark.parametrize("value", ["", "../x", "x'; DROP TABLE y;--", "x/y", "x\n"])
def test_bad_result_ids(value):
    with pytest.raises(ValueError):
        result_identifier(value)


def test_kerberos_private_cache_refresh_and_cleanup(config, monkeypatch):
    commands = []

    def run(args, **kwargs):
        commands.append((args, kwargs))
        if args[0] == "kinit":
            Path(kwargs["env"]["KRB5CCNAME"].removeprefix("FILE:")).touch()
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(subprocess, "run", run)
    original = os.environ.get("KRB5CCNAME")
    with Kerberos(config) as auth:
        cache = Path(auth.cache.removeprefix("FILE:"))
        auth.ensure()
        deadline = time.monotonic() + 1
        while sum(cmd[0][0] == "kinit" for cmd in commands) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert sum(cmd[0][0] == "kinit" for cmd in commands) >= 2
        assert cache.stat().st_mode & 0o777 == 0o600
        assert cache.parent.stat().st_mode & 0o777 == 0o700
    assert not cache.exists()
    assert auth._thread is None
    assert os.environ.get("KRB5CCNAME") == original
    assert all("shell" not in kwargs for _, kwargs in commands)


def test_kinit_failure_is_sanitized_and_cleans_up(config, monkeypatch):
    def fail(*args, **kwargs):
        raise subprocess.CalledProcessError(1, ["secret"], stderr=b"SECRET")

    monkeypatch.setattr(subprocess, "run", fail)
    with pytest.raises(AuthenticationError) as exc:
        with Kerberos(config):
            pass
    assert "SECRET" not in str(exc.value)
    assert not list(Path(config.cache_dir).glob("hrw-krb-*"))


def test_refresh_failure_blocks_operations(config, monkeypatch):
    def run(args, **kwargs):
        if args[0] == "kinit":
            Path(kwargs["env"]["KRB5CCNAME"].removeprefix("FILE:")).touch()

    monkeypatch.setattr(subprocess, "run", run)
    with Kerberos(config) as auth:
        monkeypatch.setattr(auth, "_run", Mock(side_effect=AuthenticationError("failed")))
        with pytest.raises(AuthenticationError):
            auth.refresh()
        with pytest.raises(AuthenticationError):
            auth.ensure()


def test_expired_ticket_is_reacquired_before_operation(config, monkeypatch):
    def run(args, **kwargs):
        if args[0] == "kinit":
            Path(kwargs["env"]["KRB5CCNAME"].removeprefix("FILE:")).touch()

    monkeypatch.setattr(subprocess, "run", run)
    with Kerberos(config) as auth:
        original = auth._run
        expired = [True]
        calls = []

        def ticket_check(*args):
            calls.append(args[0])
            if args[0] == "klist" and expired:
                expired.pop()
                raise AuthenticationError("expired")
            return original(*args)

        monkeypatch.setattr(auth, "_run", ticket_check)
        auth.ensure()
        assert calls == ["klist", "kinit", "klist"]


def test_hdfs_uses_current_cache_without_shell(config, monkeypatch):
    auth = Mock(config=config, env={"KRB5CCNAME": "FILE:/private/cache"})
    run = Mock(return_value=subprocess.CompletedProcess([], 0, stdout=b"12\n"))
    monkeypatch.setattr(subprocess, "run", run)
    assert Hdfs(auth).size("hdfs://cluster/writer/file") == 12
    assert run.call_args.args[0] == ["hdfs", "dfs", "-stat", "%b", "hdfs://cluster/writer/file"]
    assert run.call_args.kwargs["env"]["KRB5CCNAME"] == "FILE:/private/cache"
    auth.ensure.assert_called_once()


def test_hdfs_missing_is_only_reported_after_successful_listing(config, monkeypatch):
    auth = Mock(config=config, env={})
    run = Mock(return_value=subprocess.CompletedProcess([], 0, stdout=b""))
    monkeypatch.setattr(subprocess, "run", run)
    fs = Hdfs(auth)
    assert not fs.exists("hdfs://cluster/writer/_commits/absent.json")
    assert run.call_args.args[0][-1] == "hdfs://cluster/writer/_commits"


def test_hdfs_diagnose_does_not_call_sql(config, monkeypatch):
    from hadoop_result_writer import Connection

    connection = Connection(config)
    monkeypatch.setattr(connection.kerberos, "ensure", lambda: None)
    run = Mock(return_value=b"")
    monkeypatch.setattr(connection.hdfs, "_run", run)
    monkeypatch.setattr(connection.trino, "query", Mock(side_effect=AssertionError("SQL called")))
    assert connection.diagnose()["hdfs"] == "ok (read-only probe)"
    run.assert_called_once_with("-ls", "-d", config.hdfs_root)


@pytest.mark.parametrize(
    "option,value",
    [
        ("refresh_seconds", float("nan")),
        ("command_timeout", float("inf")),
        ("mode", "unknown"),
        ("table", "x;DROP TABLE y"),
    ],
)
def test_config_rejects_unsafe_values(config, option, value):
    from dataclasses import asdict

    with pytest.raises(ValueError):
        Config(**{**asdict(config), option: value})
