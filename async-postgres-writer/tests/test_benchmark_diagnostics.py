import asyncio
from datetime import datetime
from decimal import Decimal
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import asyncpg
import pytest

spec = spec_from_file_location(
    "stream_load", Path(__file__).parents[1] / "benchmarks" / "stream_load.py"
)
benchmark = module_from_spec(spec)
spec.loader.exec_module(benchmark)


def test_counter_records_are_json_numeric_and_do_not_include_identity_or_timestamps():
    assert benchmark.numeric_counters(
        {
            "datid": 123,
            "wal_bytes": Decimal("12345678901234567890"),
            "write_time": Decimal("1.25"),
            "stats_reset": datetime.now(),
            "datname": "test",
            "numbackends": 3,
        }
    ) == {"wal_bytes": 12345678901234567890, "write_time": 1.25, "numbackends": 3}


def test_counter_deltas_exclude_backend_gauge_and_new_counters():
    assert benchmark.counter_deltas(
        {"wal": {"wal_bytes": 100, "wal_sync": 2}, "database": {"numbackends": 2, "temp_bytes": 1}},
        {
            "wal": {"wal_bytes": 160, "wal_sync": 3, "new_counter": 99},
            "database": {"numbackends": 4, "temp_bytes": 21},
        },
    ) == {"wal": {"wal_bytes": 60, "wal_sync": 1}, "database": {"temp_bytes": 20}}


async def test_activity_monitor_timeout_recovers_and_reports_partial_coverage():
    stop = asyncio.Event()
    result = benchmark.activity_result()

    class Monitor:
        calls = 0
        closed = False

        async def fetch(self, query):
            self.calls += 1
            if self.calls == 1:
                raise TimeoutError
            stop.set()
            return [
                {
                    "state": "active",
                    "wait_event_type": "IO",
                    "wait_event": "WALWrite",
                    "backends": 1,
                }
            ]

        async def close(self, *, timeout):
            self.closed = True

    monitor = Monitor()
    task = asyncio.create_task(benchmark.sample_activity(monitor, stop, result))
    await task
    await benchmark.stop_activity(task, monitor, stop, result)
    assert result["attempts"] == 2
    assert result["samples"] == 1
    assert result["timeouts"] == 1
    assert result["errors"] == []
    assert result["coverage"] == "partial"
    assert result["backend_sample_histogram"] == {"active / IO / WALWrite": 1}
    assert monitor.closed


async def test_activity_probe_failure_is_recorded_instead_of_raised():
    stop = asyncio.Event()
    result = benchmark.activity_result()

    class Monitor:
        async def fetch(self, query):
            raise asyncpg.ConnectionDoesNotExistError("Private server details")

        async def close(self, *, timeout):
            pass

    monitor = Monitor()
    task = asyncio.create_task(benchmark.sample_activity(monitor, stop, result))
    await task
    await benchmark.stop_activity(task, monitor, stop, result)
    assert result["attempts"] == 1
    assert result["samples"] == 0
    assert result["errors"] == ["ConnectionDoesNotExistError"]
    assert result["coverage"] == "unavailable"


async def test_activity_monitor_preserves_cancellation():
    stop = asyncio.Event()
    result = benchmark.activity_result()
    started = asyncio.Event()

    class Monitor:
        async def fetch(self, query):
            started.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(benchmark.sample_activity(Monitor(), stop, result))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_activity_shutdown_deadlines_are_recorded_and_connection_terminated(monkeypatch):
    result = benchmark.activity_result()
    stop = asyncio.Event()

    class Monitor:
        terminated = False

        async def close(self, *, timeout):
            assert timeout == 5
            raise TimeoutError

        def terminate(self):
            self.terminated = True

    async def expired(task, *, timeout):
        assert timeout == 6
        raise TimeoutError

    monitor = Monitor()
    task = asyncio.create_task(asyncio.Event().wait())
    monkeypatch.setattr(benchmark.asyncio, "wait_for", expired)
    await benchmark.stop_activity(task, monitor, stop, result)
    assert result["shutdown_timeout"] is True
    assert result["close_error"] == "TimeoutError"
    assert result["coverage"] == "unavailable"
    assert monitor.terminated
    assert task.cancelled()
