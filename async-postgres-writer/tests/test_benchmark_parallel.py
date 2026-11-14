import asyncio
import hashlib
import json
import sys
from contextlib import AsyncExitStack
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

spec = spec_from_file_location(
    "stream_load", Path(__file__).parents[1] / "benchmarks" / "stream_load.py"
)
benchmark = module_from_spec(spec)
spec.loader.exec_module(benchmark)


@pytest.mark.parametrize("concurrency", [2, 4])
async def test_parallel_uploads_overlap_and_verify_each_receipt(concurrency):
    payload = b'{"n":1}\n'
    expected_digest = hashlib.sha256(payload).hexdigest()
    upload_ids = [uuid4() for _ in range(concurrency)]
    reached = asyncio.Barrier(concurrency)
    observed = set()

    async def body(digest, label):
        digest.update(payload)
        yield payload

    async def handle(request):
        assert request.method == "PUT"
        assert request.content == payload
        upload_id = request.url.path.rsplit("/", 1)[-1]
        observed.add(upload_id)
        await reached.wait()
        return httpx.Response(
            201,
            json={
                "upload_id": upload_id,
                "byte_count": len(payload),
                "row_count": 1,
                "sha256": expected_digest,
                "reused": False,
            },
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handle), base_url="http://benchmark.test"
    ) as client:
        async with asyncio.timeout(2):
            uploads, _, _ = await benchmark.upload_batch(
                client, {}, upload_ids, body, len(payload), 1
            )

    assert observed == {str(upload_id) for upload_id in upload_ids}
    assert [upload["receipt"]["upload_id"] for upload in uploads] == [
        str(upload_id) for upload_id in upload_ids
    ]


async def test_worker_connections_are_balanced_and_rejected_clients_close(monkeypatch):
    original_client = httpx.AsyncClient
    url = "http://benchmark.test"
    pids = iter((101, 101, 101, 102, 102))
    opened = []

    def open_client(**kwargs):
        pid = next(pids)

        def ready(request):
            assert str(request.url) == f"{url}/health/ready"
            return httpx.Response(200, headers={benchmark.WORKER_PID_HEADER: str(pid)})

        candidate = original_client(transport=httpx.MockTransport(ready), **kwargs)
        opened.append(candidate)
        return candidate

    monkeypatch.setattr(benchmark.httpx, "AsyncClient", open_client)
    async with AsyncExitStack() as stack:
        selected = await benchmark.warm_worker_clients(stack, url, {}, 4, 2)
        assert selected == [
            (101, opened[0]),
            (102, opened[3]),
            (101, opened[1]),
            (102, opened[4]),
        ]
        assert opened[2].is_closed
        assert not any(client.is_closed for _, client in selected)
    assert all(client.is_closed for client in opened)


async def test_worker_fallback_probes_concurrently_and_closes_extras(monkeypatch):
    original_client = httpx.AsyncClient
    opened = []
    burst = asyncio.Event()
    live_probes = 0

    def open_client(**kwargs):
        async def health(request):
            nonlocal live_probes
            if request.url.path == "/health/ready":
                assert live_probes == 0
            else:
                assert request.url.path == "/health/live"
                live_probes += 1
                if live_probes == 2:
                    burst.set()
                # No liveness response can identify the second worker until a peer starts.
                await burst.wait()
            pid = 101 if request.url.path == "/health/ready" else 102
            return httpx.Response(200, headers={benchmark.WORKER_PID_HEADER: str(pid)})

        candidate = original_client(transport=httpx.MockTransport(health), **kwargs)
        opened.append(candidate)
        return candidate

    monkeypatch.setattr(benchmark.httpx, "AsyncClient", open_client)
    async with AsyncExitStack() as stack, asyncio.timeout(2):
        selected = await benchmark.warm_worker_clients(stack, "http://benchmark.test", {}, 2, 2)
        assert [pid for pid, _ in selected] == [101, 102]
        assert {client for client in opened if not client.is_closed} == {
            client for _, client in selected
        }
    assert all(client.is_closed for client in opened)


@pytest.mark.parametrize("reported_pid", [101, 102])
async def test_timed_upload_verifies_worker_pid(reported_pid):
    upload_id = uuid4()
    payload = b'{"n":1}\n'
    sha = hashlib.sha256(payload).hexdigest()

    async def body(digest, label):
        digest.update(payload)
        yield payload

    def put(request):
        assert request.method == "PUT" and request.content == payload
        return httpx.Response(
            201,
            headers={benchmark.WORKER_PID_HEADER: str(reported_pid)},
            json={
                "upload_id": str(upload_id),
                "byte_count": len(payload),
                "row_count": 1,
                "sha256": sha,
                "reused": False,
            },
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(put), base_url="http://benchmark.test"
    ) as dedicated:
        args = (None, {}, [upload_id], body, len(payload), 1)
        if reported_pid != 101:
            with pytest.raises(RuntimeError, match="expected worker 101, got 102"):
                await benchmark.upload_batch(*args, workers=[(101, dedicated)])
        else:
            uploads, _, _ = await benchmark.upload_batch(*args, workers=[(101, dedicated)])
            assert uploads[0]["worker_pid"] == 101


async def test_activity_counts_overlapping_copies_across_wait_event_groups():
    stop = asyncio.Event()
    result = benchmark.activity_result()
    pids = set()

    class Monitor:
        async def fetch(self, query):
            stop.set()
            return [
                {
                    "state": "active",
                    "wait_event_type": "IO",
                    "wait_event": "WALWrite",
                    "backends": 2,
                    "active_copies": 2,
                    "pids": [101, 102],
                },
                {
                    "state": "active",
                    "wait_event_type": "Client",
                    "wait_event": "ClientRead",
                    "backends": 1,
                    "active_copies": 1,
                    "pids": [103],
                },
                {
                    "state": "idle",
                    "wait_event_type": "Client",
                    "wait_event": "ClientRead",
                    "backends": 1,
                    "active_copies": 0,
                    "pids": [104],
                },
            ]

    await benchmark.sample_activity(Monitor(), stop, result, pids)
    assert result["peak_app_backends"] == 4
    assert result["app_backend_histogram"] == {"4": 1}
    assert result["peak_active_copy_backends"] == 3
    assert result["active_copy_backend_histogram"] == {"3": 1}
    assert pids == {101, 102, 103, 104}


async def test_counts_only_checks_both_database_counts_without_reporting_readback_or_replay():
    upload_id = uuid4()

    class Connection:
        def __init__(self):
            self.counts = {"rows": 3, "uploads": 1}

        async def fetchrow(self, query, key):
            assert key == upload_id
            return self.counts

    connection = Connection()
    result = await benchmark.verify_upload_counts(connection, upload_id, 3)
    assert result["verified_row_count"] == 3
    assert result["readback_sha256"] is None
    assert result["replay_seconds"] is None
    assert result["replay_no_duplicates"] is False

    for mismatched in ({"rows": 2, "uploads": 1}, {"rows": 3, "uploads": 0}):
        connection.counts = mismatched
        with pytest.raises(AssertionError):
            await benchmark.verify_upload_counts(connection, upload_id, 3)


def test_cli_preserves_fixed_upload_id_order(monkeypatch, tmp_path):
    first, second = uuid4(), uuid4()
    output = tmp_path / "benchmark.json"
    argv = [
        "stream_load.py",
        "--size-gib",
        "5",
        "--concurrency",
        "2",
        "--output",
        str(output),
    ]

    async def run(args):
        return {"upload_ids": [str(key) for key in args.upload_ids]}

    monkeypatch.setattr(benchmark, "run", run)
    monkeypatch.setattr(sys, "argv", [*argv, "--upload-id", str(first), "--upload-id", str(second)])
    benchmark.main()
    assert json.loads(output.read_text())["upload_ids"] == [str(first), str(second)]

    for invalid in (
        ["--upload-id", str(first)],
        ["--upload-id", str(first)] * 2,
        ["--uvicorn-workers", "4"],
    ):
        monkeypatch.setattr(sys, "argv", [*argv, *invalid])
        with pytest.raises(SystemExit) as error:
            benchmark.main()
        assert error.value.code == 2
