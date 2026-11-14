"""Measure concurrent full NDJSON exports through the installed async PostgreSQL client."""

import argparse
import asyncio
import hashlib
import json
import os
import re
import resource
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from time import perf_counter, process_time
from uuid import UUID

import httpx

from async_postgres_client import AsyncPostgresClient

_MIB = 1024**2


class _MeasuredStream(httpx.AsyncByteStream):
    def __init__(self, stream: httpx.AsyncByteStream) -> None:
        self.stream = stream
        self.size = 0
        self.digest = hashlib.sha256()
        self.first_byte_at: float | None = None

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for chunk in self.stream:
            if self.first_byte_at is None:
                self.first_byte_at = perf_counter()
            self.size += len(chunk)
            self.digest.update(chunk)
            yield chunk

    async def aclose(self) -> None:
        await self.stream.aclose()


class _MeasuredTransport(httpx.AsyncHTTPTransport):
    def __init__(self, concurrency: int) -> None:
        super().__init__(
            limits=httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency),
            trust_env=False,
        )
        self.exports: dict[asyncio.Task, _MeasuredStream] = {}

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await super().handle_async_request(request)
        if (
            request.method == "GET"
            and request.url.path.endswith("/rows/stream")
            and response.status_code == 200
        ):
            task = asyncio.current_task()
            assert task is not None and isinstance(response.stream, httpx.AsyncByteStream)
            measured = _MeasuredStream(response.stream)
            self.exports[task] = measured
            response.stream = measured
        return response


async def measure(args: argparse.Namespace, token: str) -> dict:
    transport = _MeasuredTransport(args.concurrency)
    barrier = asyncio.Barrier(args.concurrency)

    async with AsyncPostgresClient(
        args.url, token, timeout=args.timeout, transport=transport
    ) as client:
        receipt = await client.get_upload_receipt(args.upload_id)
        if receipt.row_count != args.expected_rows:
            raise RuntimeError(f"Unexpected receipt row count: {receipt.row_count}")

        async def export() -> dict:
            await barrier.wait()
            count = 0
            first_row_at = None
            start = perf_counter()
            async with client.stream_upload_rows(args.upload_id) as rows:
                async for row in rows:
                    if first_row_at is None:
                        first_row_at = perf_counter()
                    count += 1
                    if row.row_number != count:
                        raise RuntimeError(f"Expected row {count}, received {row.row_number}")
            elapsed = perf_counter() - start
            wire = transport.exports.pop(asyncio.current_task())
            if count != receipt.row_count or first_row_at is None or wire.first_byte_at is None:
                raise RuntimeError(f"Incomplete export: {count} rows")
            return {
                "seconds": elapsed,
                "ttfb_seconds": wire.first_byte_at - start,
                "first_row_seconds": first_row_at - start,
                "rows": count,
                "bytes": wire.size,
                "sha256": wire.digest.hexdigest(),
                "rows_per_second": count / elapsed,
                "mib_per_second": wire.size / _MIB / elapsed,
            }

        cpu_start, start = process_time(), perf_counter()
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(export()) for _ in range(args.concurrency)]
        elapsed = perf_counter() - start
        cpu = process_time() - cpu_start

    streams = [task.result() for task in tasks]
    expected_bytes = args.expected_bytes or streams[0]["bytes"]
    expected_sha256 = args.expected_sha256 or streams[0]["sha256"]
    for i, stream in enumerate(streams, 1):
        if (stream["bytes"], stream["sha256"]) != (expected_bytes, expected_sha256):
            raise RuntimeError(f"Export {i} differs from reference bytes or SHA-256")
    rows = sum(stream["rows"] for stream in streams)
    size = sum(stream["bytes"] for stream in streams)
    return {
        "label": args.label,
        "upload_id": str(args.upload_id),
        "concurrent_export": {
            "concurrency": args.concurrency,
            "seconds": elapsed,
            "rows": rows,
            "bytes": size,
            "rows_per_second": rows / elapsed,
            "mib_per_second": size / _MIB / elapsed,
            "client_cpu_seconds": cpu,
            # macOS reports bytes; Linux reports KiB.
            "client_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            * (1 if sys.platform == "darwin" else 1024),
            "streams": streams,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--upload-id", required=True, type=UUID)
    parser.add_argument("--label", default="sdk")
    parser.add_argument("--concurrency", type=int, choices=(1, 2, 4), default=1)
    parser.add_argument("--expected-rows", type=int, default=9_000_000)
    parser.add_argument("--expected-bytes", type=int)
    parser.add_argument("--expected-sha256")
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    token = os.getenv("WRITER_API_TOKEN")
    if not token:
        parser.error("set WRITER_API_TOKEN")
    if args.expected_rows < 1 or (args.expected_bytes is not None and args.expected_bytes < 1):
        parser.error("expected row and byte counts must be positive")
    if args.expected_sha256 is not None and not re.fullmatch(r"[0-9a-f]{64}", args.expected_sha256):
        parser.error("--expected-sha256 must be 64 lowercase hex characters")
    result = json.dumps(asyncio.run(measure(args, token)), indent=2) + "\n"
    if args.output is None:
        print(result, end="")
    else:
        args.output.write_text(result, encoding="utf-8")


if __name__ == "__main__":
    main()
