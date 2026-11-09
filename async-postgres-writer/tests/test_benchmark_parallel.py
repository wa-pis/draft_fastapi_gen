import asyncio
import hashlib
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
    assert result["peak_active_copy_backends"] == 3
    assert result["active_copy_backend_histogram"] == {"3": 1}
    assert pids == {101, 102, 103, 104}
