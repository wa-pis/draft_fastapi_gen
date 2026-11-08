"""Local end-to-end benchmark; requires an empty, dedicated service database."""

import argparse
import asyncio
import base64
import hashlib
import importlib.metadata
import json
import os
import platform
import random
import shutil
import subprocess
import time
from contextlib import suppress
from pathlib import Path
from uuid import uuid4

import asyncpg
import httpx

ROW_BYTES = 4096
CHUNK_BYTES = 64 * 1024
GIB = 1024**3
SEED = 20261108


def row_bytes(number: int, generator: random.Random) -> bytes:
    prefix = b'{"n":' + str(number).encode() + b',"payload":"'
    length = ROW_BYTES - len(prefix) - 3
    payload = base64.b64encode(generator.randbytes((length + 3) // 4 * 3))[:length]
    return prefix + payload + b'"}\n'


def rss_bytes(pid: int) -> int:
    result = subprocess.run(
        ["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True, check=False
    )
    return int(result.stdout.strip() or 0) * 1024


async def run(args: argparse.Namespace) -> dict:
    token = os.environ["WRITER_API_TOKEN"]
    connection = await asyncpg.connect(
        os.environ["WRITER_TEST_DATABASE_URL"],
        server_settings={"application_name": "stream_load_monitor"},
        command_timeout=60,
    )
    try:
        if await connection.fetchval("SELECT EXISTS(SELECT FROM stream_writer.uploads)"):
            raise RuntimeError("Benchmark requires an empty dedicated database")
        return await measure(args, token, connection)
    finally:
        await connection.close()


async def measure(args: argparse.Namespace, token: str, connection: asyncpg.Connection) -> dict:
    total_bytes = args.size_gib * GIB
    total_rows = total_bytes // ROW_BYTES
    upload_id = uuid4()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/x-ndjson"}
    path = f"/v1/uploads/{upload_id}"
    peaks = {"api_rss_bytes": 0, "client_rss_bytes": 0, "max_pg_backend_rss_bytes": 0}
    min_free = shutil.disk_usage(args.disk_path).free
    stop = asyncio.Event()
    low_disk = asyncio.Event()
    pg_pids = set()

    async def sample():
        nonlocal min_free
        while not stop.is_set():
            peaks["api_rss_bytes"] = max(peaks["api_rss_bytes"], rss_bytes(args.server_pid))
            peaks["client_rss_bytes"] = max(peaks["client_rss_bytes"], rss_bytes(os.getpid()))
            for pid in pg_pids:
                peaks["max_pg_backend_rss_bytes"] = max(
                    peaks["max_pg_backend_rss_bytes"], rss_bytes(pid)
                )
            min_free = min(min_free, shutil.disk_usage(args.disk_path).free)
            if min_free < 20 * GIB:
                low_disk.set()
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=0.5)

    async def discover_backends():
        for _ in range(40):
            pids = await connection.fetch(
                "SELECT pid FROM pg_stat_activity WHERE datname=current_database() "
                "AND application_name='stream_writer_benchmark'"
            )
            pg_pids.update(row["pid"] for row in pids)
            if pg_pids:
                return
            await asyncio.sleep(0.25)

    async def body(digest, label):
        generator = random.Random(SEED)
        chunk = bytearray()
        for number in range(1, total_rows + 1):
            if low_disk.is_set():
                raise RuntimeError("Benchmark aborted: free disk fell below 20 GiB")
            chunk.extend(row_bytes(number, generator))
            if len(chunk) == CHUNK_BYTES:
                digest.update(chunk)
                yield bytes(chunk)
                chunk.clear()
            if number % (256 * 1024**2 // ROW_BYTES) == 0:
                print(f"{label}: {number * ROW_BYTES / GIB:.2f}/{args.size_gib} GiB", flush=True)
        if chunk:
            digest.update(chunk)
            yield bytes(chunk)

    async def sizes():
        row = await connection.fetchrow(
            "SELECT pg_database_size(current_database()) AS database_bytes, "
            "pg_total_relation_size('stream_writer.rows') AS rows_relation_bytes, "
            "pg_total_relation_size('stream_writer.uploads') AS receipts_relation_bytes"
        )
        return dict(row)

    before = await sizes()
    settings = dict(
        await connection.fetch(
            "SELECT name, setting FROM pg_settings WHERE name = ANY($1::text[])",
            [
                "fsync",
                "synchronous_commit",
                "full_page_writes",
                "shared_buffers",
                "block_size",
                "max_wal_size",
                "checkpoint_timeout",
                "wal_compression",
                "data_checksums",
            ],
        )
    )
    lsn_before = await connection.fetchval("SELECT pg_current_wal_insert_lsn()")
    sampler = asyncio.create_task(sample())
    try:
        async with httpx.AsyncClient(base_url=args.url, timeout=3600) as client:
            assert (await client.get("/health/ready", headers=headers)).status_code == 200
            await discover_backends()
            if not pg_pids:
                raise RuntimeError("Set the API DSN application_name=stream_writer_benchmark")
            digest = hashlib.sha256()
            started = time.monotonic()
            response = await client.put(path, headers=headers, content=body(digest, "upload"))
            upload_seconds = time.monotonic() - started
            if response.status_code != 201:
                raise RuntimeError(f"Upload failed: {response.status_code} {response.text}")
            receipt = response.json()
            assert receipt["upload_id"] == str(upload_id)
            assert receipt["byte_count"] == total_bytes
            assert receipt["row_count"] == total_rows
            assert receipt["sha256"] == digest.hexdigest()
            assert receipt["reused"] is False
            assert (await client.get(path, headers=headers)).json() == receipt
            lsn_after = await connection.fetchval("SELECT pg_current_wal_insert_lsn()")
            wal_bytes = await connection.fetchval(
                "SELECT pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn)", lsn_after, lsn_before
            )
            after = await sizes()
            stored_digest = hashlib.sha256()
            stored_rows = 0
            read_started = time.monotonic()
            async with connection.transaction(readonly=True):
                async for row in connection.cursor(
                    "SELECT row_number, data FROM stream_writer.rows "
                    "WHERE upload_id=$1 ORDER BY row_number",
                    upload_id,
                    prefetch=256,
                ):
                    stored_rows += 1
                    data = json.loads(row["data"])
                    assert set(data) == {"n", "payload"}
                    assert row["row_number"] == stored_rows == data["n"]
                    encoded = (
                        json.dumps(
                            {"n": data["n"], "payload": data["payload"]}, separators=(",", ":")
                        ).encode()
                        + b"\n"
                    )
                    assert len(encoded) == ROW_BYTES
                    stored_digest.update(encoded)
                    if stored_rows % (256 * 1024**2 // ROW_BYTES) == 0:
                        print(
                            f"readback: {stored_rows * ROW_BYTES / GIB:.2f}/{args.size_gib} GiB",
                            flush=True,
                        )
            read_seconds = time.monotonic() - read_started
            assert stored_rows == total_rows and stored_digest.hexdigest() == receipt["sha256"]
            replay_seconds = None
            if args.replay:
                replay_digest = hashlib.sha256()
                started = time.monotonic()
                repeated = await client.put(
                    path, headers=headers, content=body(replay_digest, "replay")
                )
                replay_seconds = time.monotonic() - started
                assert repeated.status_code == 200, repeated.text
                assert repeated.json() == dict(receipt, reused=True)
                assert replay_digest.hexdigest() == receipt["sha256"]
                assert (
                    await connection.fetchval(
                        "SELECT count(*) FROM stream_writer.rows WHERE upload_id=$1", upload_id
                    )
                    == total_rows
                )
    finally:
        stop.set()
        await sampler
    if low_disk.is_set():
        raise RuntimeError("Free disk fell below the benchmark reserve")
    if any(value <= 0 for value in peaks.values()):
        raise RuntimeError("A process memory measurement is missing")
    return {
        "size_gib": args.size_gib,
        "row_bytes": ROW_BYTES,
        "seed": SEED,
        "receipt": receipt,
        "upload_seconds": round(upload_seconds, 3),
        "upload_mib_per_second": round(total_bytes / 1024**2 / upload_seconds, 3),
        "readback_seconds": round(read_seconds, 3),
        "readback_sha256": stored_digest.hexdigest(),
        "readback_row_count": stored_rows,
        "replay_seconds": round(replay_seconds, 3) if replay_seconds is not None else None,
        "replay_no_duplicates": args.replay,
        "sampled_peaks": peaks,
        "sampling_interval_seconds": 0.5,
        "memory_sampling_scope": "upload, readback and replay",
        "disk_min_free_bytes": min_free,
        "generated_wal_bytes": int(wal_bytes),
        "sizes_before": before,
        "sizes_after": after,
        "postgres_settings": settings,
        "postgres_version": await connection.fetchval("SELECT version()"),
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "asyncpg_version": importlib.metadata.version("asyncpg"),
        "application_version": importlib.metadata.version("async-postgres-writer"),
    }


def main():
    if not __debug__:
        raise RuntimeError("Run without -O: benchmark integrity checks require assertions")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--size-gib", type=int, choices=(1, 5), required=True)
    parser.add_argument("--server-pid", type=int, required=True)
    parser.add_argument("--disk-path", default="/private/tmp")
    parser.add_argument("--replay", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = asyncio.run(run(args))
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2), flush=True)


if __name__ == "__main__":
    main()
