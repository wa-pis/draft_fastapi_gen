"""Seed a dedicated database and compare PostgreSQL HTTP read implementations."""

import argparse
import asyncio
import hashlib
import json
import os
import resource
import sys
from decimal import Decimal
from pathlib import Path
from statistics import median
from time import perf_counter, process_time
from uuid import UUID

import asyncpg
import httpx

_DEFAULT_ROWS = 9_000_000
_MIB = 1024**2
_SEED = r"""
INSERT INTO stream_writer.rows (upload_id, row_number, data)
SELECT $1, n, jsonb_build_object(
    'calculation_id', 'calc-' || (n % 100000),
    'account_id', 10000000 + n % 1000000,
    'status', (ARRAY['ready', 'paid', 'partial'])[1 + n % 3],
    'region', (ARRAY['RU', 'KZ', 'AM'])[1 + n % 3],
    'amount', (n / 100.0 + 0.1234567890123456789)::numeric(32, 19),
    'valid', n % 11 <> 0,
    'description', md5(n::text) || md5((n + $2)::text) ||
        CASE WHEN n % 512 = 0 THEN E'\\path\t"результат"\n' ELSE '' END
)
FROM generate_series(1::bigint, $2::bigint) AS s(n)
"""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


async def seed(dsn: str, upload_id: UUID, rows: int) -> dict:
    conn = await asyncpg.connect(dsn)
    try:
        async with conn.transaction():
            # Refuse concurrent writers and nonempty service databases before inserting anything.
            await conn.execute(
                "LOCK TABLE stream_writer.uploads, stream_writer.rows IN ACCESS EXCLUSIVE MODE NOWAIT"
            )
            _require(
                not await conn.fetchval("SELECT EXISTS (SELECT FROM stream_writer.uploads)"),
                "Seed requires an initialized, empty test database",
            )
            await conn.execute("SET LOCAL synchronous_commit=on")
            # There is no PUT payload: zeroes mark the receipt as a synthetic read fixture.
            await conn.execute(
                "INSERT INTO stream_writer.uploads (upload_id, sha256, row_count, byte_count) "
                "VALUES ($1, $2, $3, 0)",
                upload_id,
                "0" * 64,
                rows,
            )
            _require(await conn.execute(_SEED, upload_id, rows) == f"INSERT 0 {rows}", "Bad seed")
        await conn.execute("ANALYZE stream_writer.rows")
        return {
            "upload_id": str(upload_id),
            "seeded_rows": rows,
            "fixture": "deterministic JSONB via SQL generate_series (synthetic receipt)",
            "seed_sql_sha256": hashlib.sha256(_SEED.encode()).hexdigest(),
            "postgres_version": await conn.fetchval("SHOW server_version"),
            "rows_relation_bytes": await conn.fetchval(
                "SELECT pg_total_relation_size('stream_writer.rows')"
            ),
        }
    finally:
        await conn.close()


async def measure_export(client: httpx.AsyncClient, path: str, expected_rows: int) -> dict:
    count = size = 0
    first = None
    digest = hashlib.sha256()
    start = perf_counter()
    async with client.stream("GET", path + "/rows/stream") as response:
        response.raise_for_status()
        _require(
            response.headers["content-type"].split(";")[0] == "application/x-ndjson"
            and response.headers["x-row-count"] == str(expected_rows),
            "Wrong export headers",
        )
        async for chunk in response.aiter_raw():
            if first is None:
                first = perf_counter()
            count += chunk.count(b"\n")
            size += len(chunk)
            digest.update(chunk)
    elapsed = perf_counter() - start
    _require(first is not None and count == expected_rows, f"Incomplete export: {count} lines")
    return {
        "seconds": elapsed,
        "ttfb_seconds": first - start,
        "rows": count,
        "bytes": size,
        "sha256": digest.hexdigest(),
        "rows_per_second": count / elapsed,
        "mib_per_second": size / _MIB / elapsed,
    }


async def measure_concurrent_exports(
    client: httpx.AsyncClient, path: str, expected_rows: int, concurrency: int
) -> dict:
    ready = asyncio.Barrier(concurrency)

    async def export() -> dict:
        await ready.wait()
        return await measure_export(client, path, expected_rows)

    cpu_start, start = process_time(), perf_counter()
    async with asyncio.TaskGroup() as group:
        tasks = [group.create_task(export()) for _ in range(concurrency)]
    elapsed = perf_counter() - start
    client_cpu = process_time() - cpu_start
    streams = [task.result() for task in tasks]
    rows = sum(stream["rows"] for stream in streams)
    size = sum(stream["bytes"] for stream in streams)
    return {
        "concurrency": concurrency,
        "seconds": elapsed,
        "rows": rows,
        "bytes": size,
        "rows_per_second": rows / elapsed,
        "mib_per_second": size / _MIB / elapsed,
        "client_cpu_seconds": client_cpu,
        # macOS reports bytes; Linux reports KiB.
        "client_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        * (1 if sys.platform == "darwin" else 1024),
        "streams": streams,
    }


async def measure_page(
    client: httpx.AsyncClient, path: str, expected_rows: int, limit: int, param: str, position: int
) -> tuple[float, list[dict]]:
    start = perf_counter()
    response = await client.get(path + "/rows", params={"limit": limit, param: position})
    elapsed = perf_counter() - start
    response.raise_for_status()
    page = json.loads(response.content, parse_float=Decimal)
    rows = page["rows"]
    end = min(expected_rows, position + limit)
    _require(
        page["upload_id"] == path.rsplit("/", 1)[1]
        and page["row_count"] == expected_rows
        and page["limit"] == limit
        and page[param] == position
        and [r["row_number"] for r in rows] == list(range(position + 1, end + 1)),
        f"Wrong {param} page at {position}",
    )
    if param == "after_row_number":
        _require(
            page["next_after_row_number"] == (end if end < expected_rows else None),
            f"Wrong next cursor at {position}",
        )
    return elapsed, rows


async def measure(args: argparse.Namespace) -> dict:
    path = f"/v1/uploads/{args.upload_id}"
    result = {"label": args.label, "upload_id": str(args.upload_id)}
    async with httpx.AsyncClient(
        base_url=args.url,
        headers={
            "Authorization": f"Bearer {os.environ['WRITER_API_TOKEN']}",
            "Accept-Encoding": "identity",
        },
        limits=httpx.Limits(
            max_connections=args.concurrency, max_keepalive_connections=args.concurrency
        ),
        timeout=args.timeout,
        trust_env=False,
    ) as client:
        receipt = await client.get(path)
        receipt.raise_for_status()
        _require(receipt.json()["row_count"] == args.expected_rows, "Wrong receipt row count")
        if not args.pages_only:
            result["concurrent_export"] = run = await measure_concurrent_exports(
                client, path, args.expected_rows, args.concurrency
            )
            streams = run["streams"]
            if args.concurrency == 1:
                result["stream"] = {
                    **streams[0],
                    "client_cpu_seconds": run["client_cpu_seconds"],
                    "client_peak_rss_bytes": run["client_peak_rss_bytes"],
                }
            reference = streams[0]
            if args.compare_stream:
                previous = json.loads(args.compare_stream.read_text())
                reference = (
                    previous["stream"]
                    if "stream" in previous
                    else previous["concurrent_export"]["streams"][0]
                )
            for i, stream in enumerate(streams, 1):
                _require(
                    all(reference[k] == stream[k] for k in ("rows", "bytes", "sha256")),
                    f"Export {i} differs from the reference export",
                )
        if not args.stream_only:
            result["pages"] = []
            for position in dict.fromkeys(
                (0, args.expected_rows // 2, max(0, args.expected_rows - args.page_limit))
            ):
                durations = {"offset": []}
                if args.keyset:
                    durations["after_row_number"] = []
                for repeat in range(args.page_repeats):
                    data = []
                    # Alternate ordering so one method does not always warm the other.
                    params = list(durations)
                    for param in params if repeat % 2 == 0 else reversed(params):
                        seconds, rows = await measure_page(
                            client, path, args.expected_rows, args.page_limit, param, position
                        )
                        durations[param].append(seconds)
                        data.append(rows)
                    _require(len(data) == 1 or data[0] == data[1], "Pagination forms disagree")
                result["pages"].append(
                    {
                        "position": position,
                        "limit": args.page_limit,
                        "samples_seconds": durations,
                        "median_seconds": {param: median(v) for param, v in durations.items()},
                    }
                )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    init = commands.add_parser("seed", help="Requires an initialized, empty test database")
    init.add_argument("--dsn-env", default="WRITER_TEST_DATABASE_URL")
    init.add_argument("--upload-id", type=UUID, required=True)
    init.add_argument("--rows", type=int, default=_DEFAULT_ROWS)
    read = commands.add_parser("measure")
    read.add_argument("--url", required=True)
    read.add_argument("--upload-id", type=UUID, required=True)
    read.add_argument("--label", required=True)
    read.add_argument("--expected-rows", type=int, default=_DEFAULT_ROWS)
    read.add_argument("--page-limit", type=int, default=1000)
    read.add_argument("--page-repeats", type=int, default=3)
    read.add_argument("--concurrency", type=int, default=1, help="Concurrent full exports")
    read.add_argument("--timeout", type=float, default=120)
    read.add_argument("--keyset", action="store_true")
    read.add_argument("--compare-stream", type=Path, help="Verify bytes and SHA-256 against a run")
    mode = read.add_mutually_exclusive_group()
    mode.add_argument("--stream-only", action="store_true")
    mode.add_argument("--pages-only", action="store_true")
    read.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.command == "seed":
        if args.rows <= 0 or not os.getenv(args.dsn_env):
            parser.error("--rows must be positive and the DSN environment variable must be set")
        report = asyncio.run(seed(os.environ[args.dsn_env], args.upload_id, args.rows))
    else:
        if not os.getenv("WRITER_API_TOKEN"):
            parser.error("set WRITER_API_TOKEN")
        if args.expected_rows <= 0 or not 1 <= args.page_limit <= 1000 or args.page_repeats <= 0:
            parser.error("positive row and repeat counts and a page limit of 1-1000 required")
        if args.pages_only and args.compare_stream:
            parser.error("--compare-stream requires a stream run")
        if args.concurrency < 1 or (args.concurrency > 1 and not args.stream_only):
            parser.error(
                "--concurrency requires a positive integer; above 1 requires --stream-only"
            )
        report = asyncio.run(measure(args))
    data = json.dumps(report, indent=2) + "\n"
    if getattr(args, "output", None):
        args.output.write_text(data)
    else:
        print(data, end="")


if __name__ == "__main__":
    main()
