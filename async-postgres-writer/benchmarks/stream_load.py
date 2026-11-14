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
from contextlib import AsyncExitStack, suppress
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import httpx

ROW_BYTES = 4096
CHUNK_BYTES = 64 * 1024
GIB = 1024**3
SEED = 20261108
MONITOR_ERRORS = (asyncpg.PostgresError, asyncpg.InterfaceError, OSError)
WORKER_PID_HEADER = "X-Benchmark-Worker-Pid"


def activity_result() -> dict:
    return {
        "samples": 0,
        "attempts": 0,
        "timeouts": 0,
        "errors": [],
        "shutdown_timeout": False,
        "close_error": None,
        "coverage": "unavailable",
        "backend_sample_histogram": {},
        "app_backend_histogram": {},
        "peak_app_backends": 0,
        "active_copy_backend_histogram": {},
        "peak_active_copy_backends": 0,
        "interval_seconds": 0.5,
    }


def numeric_counters(record) -> dict:
    result = {}
    for key, value in dict(record).items():
        if key == "datid" or isinstance(value, bool):
            continue
        if isinstance(value, Decimal):
            value = int(value) if value == value.to_integral_value() else float(value)
        if isinstance(value, (int, float)):
            result[key] = value
    return result


def counter_deltas(before: dict, after: dict) -> dict:
    return {
        view: {
            name: value - before[view][name]
            for name, value in counters.items()
            if name != "numbackends" and name in before.get(view, {})
        }
        for view, counters in after.items()
        if view in before
    }


async def counter_snapshot(connection: asyncpg.Connection) -> dict:
    version = int(await connection.fetchval("SHOW server_version_num"))
    views = ["pg_stat_bgwriter", "pg_stat_wal"]
    if version >= 170000:
        views.append("pg_stat_checkpointer")
    result = {}
    for view in views:
        result[view] = numeric_counters(await connection.fetchrow(f"SELECT * FROM {view}"))
    result["pg_stat_database"] = numeric_counters(
        await connection.fetchrow("SELECT * FROM pg_stat_database WHERE datname=current_database()")
    )
    return result


async def sample_activity(
    connection: asyncpg.Connection, stop: asyncio.Event, result: dict, pg_pids=None
):
    while not stop.is_set():
        result["attempts"] += 1
        try:
            rows = await connection.fetch(
                "SELECT a.state, a.wait_event_type, a.wait_event, count(*) AS backends, "
                "count(c.pid) AS active_copies, array_agg(a.pid) AS pids "
                "FROM pg_stat_activity AS a LEFT JOIN pg_stat_progress_copy AS c USING (pid) "
                "WHERE a.datname=current_database() "
                "AND a.application_name='stream_writer_benchmark' "
                "GROUP BY a.state, a.wait_event_type, a.wait_event"
            )
        except TimeoutError:
            result["timeouts"] += 1
        except MONITOR_ERRORS as exc:
            result["errors"].append(type(exc).__name__)
            break
        else:
            result["samples"] += 1
            backends = sum(row["backends"] for row in rows)
            app_histogram = result["app_backend_histogram"]
            app_histogram[str(backends)] = app_histogram.get(str(backends), 0) + 1
            result["peak_app_backends"] = max(result["peak_app_backends"], backends)
            copies = sum(row.get("active_copies", 0) for row in rows)
            copy_histogram = result["active_copy_backend_histogram"]
            copy_histogram[str(copies)] = copy_histogram.get(str(copies), 0) + 1
            result["peak_active_copy_backends"] = max(result["peak_active_copy_backends"], copies)
            for row in rows:
                if pg_pids is not None:
                    pg_pids.update(row["pids"])
                key = " / ".join(
                    row[name] or "none" for name in ("state", "wait_event_type", "wait_event")
                )
                result["backend_sample_histogram"][key] = (
                    result["backend_sample_histogram"].get(key, 0) + row["backends"]
                )
        with suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=0.5)


async def stop_activity(task, connection, stop: asyncio.Event, result: dict):
    stop.set()
    try:
        await asyncio.wait_for(task, timeout=6)
    except TimeoutError:
        result["shutdown_timeout"] = True
    except MONITOR_ERRORS as exc:
        result["errors"].append(type(exc).__name__)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        try:
            await connection.close(timeout=5)
        except (TimeoutError, *MONITOR_ERRORS) as exc:
            result["close_error"] = type(exc).__name__
            connection.terminate()
        failed = (
            result["timeouts"]
            or result["errors"]
            or result["shutdown_timeout"]
            or result["close_error"]
        )
        result["coverage"] = (
            "unavailable" if not result["samples"] else "partial" if failed else "complete"
        )


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


def stream_label(phase: str, index: int, concurrency: int) -> str:
    return phase if concurrency == 1 else f"{phase} {index + 1}/{concurrency}"


def response_worker_pid(response: httpx.Response) -> int:
    try:
        pid = int(response.headers[WORKER_PID_HEADER])
    except (KeyError, ValueError) as exc:
        raise RuntimeError(f"Missing or invalid {WORKER_PID_HEADER}") from exc
    if pid < 1:
        raise RuntimeError(f"Invalid {WORKER_PID_HEADER}: {pid}")
    return pid


async def warm_worker_clients(stack, url, headers, concurrency, worker_count):
    per_worker = concurrency // worker_count
    by_pid = {}
    limits = httpx.Limits(max_connections=1, max_keepalive_connections=1, keepalive_expiry=3600)

    async def probe(attempt, path):
        candidate = await attempt.enter_async_context(
            httpx.AsyncClient(base_url=url, timeout=3600, limits=limits)
        )
        response = await candidate.get(path, headers=headers, timeout=10)
        if response.status_code != 200:
            raise RuntimeError(f"Worker probe {path} failed: HTTP {response.status_code}")
        if response.headers.get("Connection", "").lower() == "close":
            raise RuntimeError(f"Worker probe {path} did not keep its HTTP connection open")
        return response_worker_pid(response), candidate, attempt

    def retain(pid, candidate, attempt):
        clients = by_pid.setdefault(pid, [])
        if len(by_pid) > worker_count:
            raise RuntimeError(
                f"Expected {worker_count} Uvicorn workers but observed {len(by_pid)}"
            )
        if len(clients) < per_worker:
            clients.append(candidate)
            stack.push_async_callback(attempt.pop_all().aclose)

    def balanced():
        if len(by_pid) == worker_count and all(
            len(clients) == per_worker for clients in by_pid.values()
        ):
            # Distribute consecutive upload slots across PIDs before reusing a worker.
            return [
                (pid, clients[slot])
                for slot in range(per_worker)
                for pid, clients in by_pid.items()
            ]
        return None

    for _ in range(max(32, 16 * concurrency)):
        async with AsyncExitStack() as attempt:
            retain(*(await probe(attempt, "/health/ready")))
            if selected := balanced():
                return selected
    # Shared-port accept can repeatedly favor the same processes on serial connections.
    # Probe liveness in bursts so concurrent warmup never queues checks on the DB pools.
    for _ in range(3):
        async with AsyncExitStack() as burst:
            attempts = [await burst.enter_async_context(AsyncExitStack()) for _ in range(24)]
            async with asyncio.TaskGroup() as group:
                tasks = [group.create_task(probe(attempt, "/health/live")) for attempt in attempts]
            for task in tasks:
                retain(*task.result())
            if selected := balanced():
                return selected
    observed = {pid: len(clients) for pid, clients in by_pid.items()}
    raise RuntimeError(f"Could not warm {per_worker} connections per worker: {observed}")


async def upload_batch(client, headers, upload_ids, body, total_bytes, total_rows, *, workers=None):
    async def put(index, upload_id):
        digest = hashlib.sha256()
        label = stream_label("upload", index, len(upload_ids))
        expected_pid, target = workers[index] if workers is not None else (None, client)
        started = time.monotonic()
        response = await target.put(
            f"/v1/uploads/{upload_id}", headers=headers, content=body(digest, label)
        )
        finished = time.monotonic()
        wall_finished = time.time()
        if response.status_code != 201:
            raise RuntimeError(f"{label} failed: {response.status_code} {response.text}")
        worker = {}
        if expected_pid is not None:
            actual_pid = response_worker_pid(response)
            if actual_pid != expected_pid:
                raise RuntimeError(f"{label} expected worker {expected_pid}, got {actual_pid}")
            worker["worker_pid"] = actual_pid
        receipt = response.json()
        assert receipt["upload_id"] == str(upload_id)
        assert receipt["byte_count"] == total_bytes
        assert receipt["row_count"] == total_rows
        assert receipt["sha256"] == digest.hexdigest()
        assert receipt["reused"] is False
        elapsed = finished - started
        return (
            {
                "receipt": receipt,
                "upload_seconds": round(elapsed, 3),
                "upload_mib_per_second": round(total_bytes / 1024**2 / elapsed, 3),
                **worker,
            },
            finished,
            wall_finished,
        )

    if len(upload_ids) == 1:
        results = [await put(0, upload_ids[0])]
    else:
        # Schedule every PUT before yielding to any of them; a failure cancels its peers.
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(put(index, key)) for index, key in enumerate(upload_ids)]
        results = [task.result() for task in tasks]
    _, finished, wall_finished = max(results, key=lambda result: result[1])
    return [result[0] for result in results], finished, wall_finished


async def verify_upload_counts(connection, upload_id, total_rows, table="stream_writer.rows"):
    counts = await connection.fetchrow(
        f"SELECT (SELECT count(*) FROM {table} WHERE upload_id=$1) AS rows, "
        "(SELECT count(*) FROM stream_writer.uploads WHERE upload_id=$1) AS uploads",
        upload_id,
    )
    assert counts["rows"] == total_rows and counts["uploads"] == 1
    return {
        "verified_row_count": counts["rows"],
        "readback_seconds": None,
        "readback_sha256": None,
        "readback_row_count": None,
        "replay_seconds": None,
        "replay_no_duplicates": False,
    }


def verify_mixed_row(layout, row, upload_id, row_number, encoded):
    from typed_data import TYPED_COLUMNS, typed_record

    assert row["row_number"] == row_number
    if layout == "columns":
        expected = typed_record(upload_id, row_number, encoded.decode())
        for column, value in zip(TYPED_COLUMNS, expected, strict=True):
            stored = row[column]
            if column == "metrics" and value is not None:
                assert stored is not None
                assert json.loads(stored) == json.loads(value)
            else:
                assert stored == value, (row_number, column)
    else:
        assert json.loads(row["data"]) == json.loads(encoded)


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
    concurrency = getattr(args, "concurrency", 1)
    uvicorn_workers = getattr(args, "uvicorn_workers", None)
    counts_only = getattr(args, "verify_counts_only", False)
    mixed_layout = getattr(args, "mixed_layout", None)
    table = "stream_writer_bench.typed_rows" if mixed_layout == "columns" else "stream_writer.rows"
    row_generator = row_bytes
    if mixed_layout:
        from typed_data import typed_row_bytes

        row_generator = typed_row_bytes
        target = await connection.fetchrow(
            "SELECT relkind::text, relpersistence::text, EXISTS ("
            "SELECT FROM pg_constraint WHERE conrelid=pg_class.oid AND contype='f' "
            "AND convalidated AND confrelid='stream_writer.uploads'::regclass) "
            "FROM pg_class WHERE oid=$1::text::regclass",
            table,
        )
        if tuple(target) != ("r", "p", True) or await connection.fetchval(
            f"SELECT EXISTS(SELECT FROM {table})"
        ):
            raise RuntimeError(
                "Mixed benchmark requires a fresh, logged, unpartitioned row table with receipt FK"
            )
    upload_ids = getattr(args, "upload_ids", None) or [uuid4() for _ in range(concurrency)]
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/x-ndjson"}
    server_rss = args.server_pid is not None
    peaks = {"client_rss_bytes": 0}
    if server_rss:
        peaks.update(api_rss_bytes=0, max_pg_backend_rss_bytes=0)
    min_disk_free_gib = getattr(args, "min_disk_free_gib", 20)
    disk_reserve_bytes = min_disk_free_gib * GIB
    min_free = shutil.disk_usage(args.disk_path).free
    stop = asyncio.Event()
    low_disk = asyncio.Event()
    pg_pids = set()
    diagnostics_enabled = getattr(args, "diagnostics", False)
    diagnostics = {
        "scope": (
            "Client progress/activity during first PUT; client yields do not confirm commit"
            if concurrency == 1
            else "Activity during concurrent PUTs; no per-stream client yield timing"
        ),
        "counter_scope": "pg_stat_database: current database; other views: whole cluster",
        "counter_window": (
            "Before activity monitor connect through shutdown and 1s collector settle; "
            "may include background work and publication lag"
        ),
        "activity": activity_result(),
    }
    if concurrency == 1:
        diagnostics["client_progress"] = []
    upload_started = None
    input_yield_finished = None
    client_yield_wait_seconds = 0.0

    async def sample():
        nonlocal min_free
        while not stop.is_set():
            peaks["client_rss_bytes"] = max(peaks["client_rss_bytes"], rss_bytes(os.getpid()))
            if server_rss:
                peaks["api_rss_bytes"] = max(peaks["api_rss_bytes"], rss_bytes(args.server_pid))
                for pid in pg_pids:
                    peaks["max_pg_backend_rss_bytes"] = max(
                        peaks["max_pg_backend_rss_bytes"], rss_bytes(pid)
                    )
            min_free = min(min_free, shutil.disk_usage(args.disk_path).free)
            if min_free < disk_reserve_bytes:
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
        nonlocal input_yield_finished, client_yield_wait_seconds
        generator = random.Random(SEED)
        chunk = bytearray()
        for number in range(1, total_rows + 1):
            if low_disk.is_set():
                raise RuntimeError(
                    f"Benchmark aborted: free disk fell below {min_disk_free_gib} GiB"
                )
            chunk.extend(row_generator(number, generator))
            if len(chunk) == CHUNK_BYTES:
                digest.update(chunk)
                yielded_at = time.monotonic() if diagnostics_enabled and label == "upload" else None
                yield bytes(chunk)
                if yielded_at is not None:
                    client_yield_wait_seconds += time.monotonic() - yielded_at
                chunk.clear()
            if number % (256 * 1024**2 // ROW_BYTES) == 0:
                message = f"{label}: {number * ROW_BYTES / GIB:.2f}/{args.size_gib} GiB"
                if diagnostics_enabled and label == "upload":
                    elapsed = time.monotonic() - upload_started
                    diagnostics["client_progress"].append(
                        {
                            "input_bytes": number * ROW_BYTES,
                            "elapsed_seconds": round(elapsed, 6),
                            "client_yield_wait_seconds": round(client_yield_wait_seconds, 6),
                        }
                    )
                    message += f"; client yield elapsed {elapsed:.3f}s"
                print(message, flush=True)
        if chunk:
            digest.update(chunk)
            yielded_at = time.monotonic() if diagnostics_enabled and label == "upload" else None
            yield bytes(chunk)
            if yielded_at is not None:
                client_yield_wait_seconds += time.monotonic() - yielded_at
        if diagnostics_enabled and label == "upload":
            input_yield_finished = time.monotonic()

    async def sizes():
        row = await connection.fetchrow(
            "SELECT pg_database_size(current_database()) AS database_bytes, "
            "COALESCE((SELECT sum(pg_total_relation_size(relid))::bigint "
            "FROM pg_partition_tree($1::text::regclass) WHERE isleaf), "
            "pg_total_relation_size($1::text::regclass)) AS rows_relation_bytes, "
            "pg_total_relation_size('stream_writer.uploads') AS receipts_relation_bytes",
            table,
        )
        result = dict(row)
        if mixed_layout:
            result["rows_toast_relation_bytes"] = await connection.fetchval(
                "SELECT pg_total_relation_size(reltoastrelid) "
                "FROM pg_class WHERE oid=$1::text::regclass",
                table,
            )
        return result

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
                "checkpoint_completion_target",
                "track_io_timing",
                "track_wal_io_timing",
                "wal_buffers",
                "wal_sync_method",
                "min_wal_size",
                "default_toast_compression",
            ],
        )
    )
    lsn_before = await connection.fetchval("SELECT pg_current_wal_insert_lsn()")
    sampler = asyncio.create_task(sample())
    try:
        async with (
            httpx.AsyncClient(base_url=args.url, timeout=3600) as client,
            AsyncExitStack() as worker_stack,
        ):
            assert (await client.get("/health/ready", headers=headers)).status_code == 200
            workers = (
                await warm_worker_clients(
                    worker_stack, args.url, headers, concurrency, uvicorn_workers
                )
                if uvicorn_workers is not None
                else None
            )
            await discover_backends()
            if not pg_pids:
                raise RuntimeError("Set the API DSN application_name=stream_writer_benchmark")
            monitor = None
            activity_task = None
            activity_stop = asyncio.Event()
            if diagnostics_enabled:
                diagnostics["counters_before"] = await counter_snapshot(connection)
            if diagnostics_enabled or (server_rss and concurrency > 1):
                try:
                    monitor = await asyncpg.connect(
                        os.environ["WRITER_TEST_DATABASE_URL"],
                        server_settings={"application_name": "stream_load_activity_monitor"},
                        command_timeout=5,
                    )
                except TimeoutError:
                    diagnostics["activity"]["timeouts"] += 1
                except MONITOR_ERRORS as exc:
                    diagnostics["activity"]["errors"].append(type(exc).__name__)
            # Прогон D исключён без «поправки» цифр: часы Python и PG разошлись.
            # Сопоставление таймеров помогает заметить аномалию, но не доказывает её причину.
            # Расхождение нужно учитывать при анализе отчёта; см. ../VALIDATION.md.
            wall_started = time.time()
            started = time.monotonic()
            client_cpu_started = time.process_time()
            upload_started = started
            if monitor is not None:
                activity_task = asyncio.create_task(
                    sample_activity(
                        monitor,
                        activity_stop,
                        diagnostics["activity"],
                        pg_pids if server_rss else None,
                    )
                )
            try:
                uploads, upload_finished, wall_finished = await upload_batch(
                    client, headers, upload_ids, body, total_bytes, total_rows, workers=workers
                )
                client_upload_cpu_seconds = time.process_time() - client_cpu_started
            finally:
                if activity_task is not None:
                    await stop_activity(
                        activity_task, monitor, activity_stop, diagnostics["activity"]
                    )
            await worker_stack.aclose()
            upload_seconds = upload_finished - started
            if diagnostics_enabled:
                diagnostics["wall_clock_upload_seconds"] = round(wall_finished - wall_started, 6)
                diagnostics["wall_minus_monotonic_seconds"] = round(
                    wall_finished - wall_started - upload_seconds, 6
                )
                # PG14's collector publishes counters asynchronously after the backend idles.
                await asyncio.sleep(1)
                diagnostics["counter_settle_seconds"] = 1
                diagnostics["counters_after"] = await counter_snapshot(connection)
                diagnostics["counter_deltas"] = counter_deltas(
                    diagnostics["counters_before"], diagnostics["counters_after"]
                )
                if concurrency == 1:
                    diagnostics["final_tail_seconds"] = (
                        round(upload_finished - input_yield_finished, 6)
                        if input_yield_finished is not None
                        else None
                    )
                    diagnostics["client_yield_wait_seconds"] = round(client_yield_wait_seconds, 6)
            for upload_id, upload in zip(upload_ids, uploads, strict=True):
                response = await client.get(f"/v1/uploads/{upload_id}", headers=headers)
                assert response.status_code == 200
                assert response.json() == upload["receipt"]
            lsn_after = await connection.fetchval("SELECT pg_current_wal_insert_lsn()")
            wal_bytes = await connection.fetchval(
                "SELECT pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn)", lsn_after, lsn_before
            )
            after = await sizes()
            select_columns = "*" if mixed_layout == "columns" else "row_number, data"
            for index, (upload_id, upload) in enumerate(zip(upload_ids, uploads, strict=True)):
                if counts_only:
                    upload.update(
                        await verify_upload_counts(connection, upload_id, total_rows, table)
                    )
                    continue
                receipt = upload["receipt"]
                stored_digest = hashlib.sha256()
                stored_rows = 0
                read_started = time.monotonic()
                read_label = stream_label("readback", index, concurrency)
                expected_generator = random.Random(SEED)
                async with connection.transaction(readonly=True):
                    async for row in connection.cursor(
                        f"SELECT {select_columns} FROM {table} WHERE upload_id=$1 ORDER BY row_number",
                        upload_id,
                        prefetch=256,
                    ):
                        stored_rows += 1
                        assert row["row_number"] == stored_rows
                        if mixed_layout:
                            encoded = row_generator(stored_rows, expected_generator)
                            verify_mixed_row(mixed_layout, row, upload_id, stored_rows, encoded)
                        else:
                            data = json.loads(row["data"])
                            assert set(data) == {"n", "payload"}
                            assert stored_rows == data["n"]
                            encoded = (
                                json.dumps(
                                    {"n": data["n"], "payload": data["payload"]},
                                    separators=(",", ":"),
                                ).encode()
                                + b"\n"
                            )
                        assert len(encoded) == ROW_BYTES
                        stored_digest.update(encoded)
                        if stored_rows % (256 * 1024**2 // ROW_BYTES) == 0:
                            print(
                                f"{read_label}: {stored_rows * ROW_BYTES / GIB:.2f}/{args.size_gib} GiB",
                                flush=True,
                            )
                read_seconds = time.monotonic() - read_started
                assert stored_rows == total_rows and stored_digest.hexdigest() == receipt["sha256"]
                replay_seconds = None
                if args.replay:
                    replay_digest = hashlib.sha256()
                    replay_started = time.monotonic()
                    repeated = await client.put(
                        f"/v1/uploads/{upload_id}",
                        headers=headers,
                        content=body(replay_digest, stream_label("replay", index, concurrency)),
                    )
                    replay_seconds = time.monotonic() - replay_started
                    assert repeated.status_code == 200, repeated.text
                    assert repeated.json() == dict(receipt, reused=True)
                    assert replay_digest.hexdigest() == receipt["sha256"]
                    assert (
                        await connection.fetchval(
                            f"SELECT count(*) FROM {table} WHERE upload_id=$1", upload_id
                        )
                        == total_rows
                    )
                upload.update(
                    readback_seconds=round(read_seconds, 3),
                    readback_sha256=stored_digest.hexdigest(),
                    readback_row_count=stored_rows,
                    replay_seconds=round(replay_seconds, 3) if replay_seconds is not None else None,
                    replay_no_duplicates=args.replay,
                )
            counts = await connection.fetchrow(
                f"SELECT (SELECT count(*) FROM {table}) AS rows, "
                "(SELECT count(*) FROM stream_writer.uploads) AS uploads"
            )
            assert counts["rows"] == total_rows * concurrency and counts["uploads"] == concurrency
    finally:
        stop.set()
        await sampler
    if low_disk.is_set():
        raise RuntimeError("Free disk fell below the benchmark reserve")
    if any(value <= 0 for value in peaks.values()):
        raise RuntimeError("A process memory measurement is missing")
    memory_scope = "upload and count verification" if counts_only else "upload, readback and replay"
    result = {
        "size_gib": args.size_gib,
        "concurrency": concurrency,
        "total_input_bytes": total_bytes * concurrency,
        "verified_total_rows": counts["rows"],
        "row_bytes": ROW_BYTES,
        "seed": SEED,
        "upload_seconds": round(upload_seconds, 3),
        "upload_mib_per_second": round(total_bytes * concurrency / 1024**2 / upload_seconds, 3),
        "client_upload_cpu_seconds": round(client_upload_cpu_seconds, 3),
        "replay_no_duplicates": args.replay,
        "sampled_peaks": peaks,
        "sampling_interval_seconds": 0.5,
        "memory_sampling_scope": memory_scope if server_rss else f"client only: {memory_scope}",
        "disk_min_free_bytes": min_free,
        "disk_reserve_bytes": disk_reserve_bytes,
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
    if concurrency == 1:
        result.update({key: value for key, value in uploads[0].items() if key not in result})
    else:
        result["uploads"] = uploads
    if counts_only:
        result["verification_mode"] = "counts_only"
    if uvicorn_workers is not None:
        result["uvicorn_workers"] = uvicorn_workers
    if mixed_layout:
        result["input_profile"] = "mixed-25"
        result["storage_layout"] = mixed_layout
        if not counts_only:
            result["verification_mode"] = "all_values_against_seeded_input"
    if diagnostics_enabled:
        result["diagnostics"] = diagnostics
    if getattr(args, "label", None) is not None:
        result["label"] = args.label
    return result


def main():
    if not __debug__:
        raise RuntimeError("Run without -O: benchmark integrity checks require assertions")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--size-gib", type=int, choices=(1, 5), required=True)
    parser.add_argument("--concurrency", type=int, choices=(1, 2, 4), default=1)
    parser.add_argument(
        "--uvicorn-workers",
        type=int,
        choices=(1, 2, 4),
        help="Warm and evenly allocate PUT connections across workers on one URL",
    )
    parser.add_argument(
        "--upload-id",
        dest="upload_ids",
        type=UUID,
        action="append",
        help="Repeat once per PUT to use fixed IDs instead of random UUIDs",
    )
    parser.add_argument(
        "--server-pid",
        type=int,
        help="Sample API and PostgreSQL RSS only when their PIDs are visible to the client",
    )
    parser.add_argument("--disk-path", default="/private/tmp")
    parser.add_argument("--min-disk-free-gib", type=int, default=20)
    parser.add_argument("--replay", action="store_true")
    parser.add_argument(
        "--verify-counts-only",
        action="store_true",
        help="Verify receipts and database row counts without full readback or replay",
    )
    parser.add_argument("--diagnostics", action="store_true")
    parser.add_argument(
        "--mixed-layout",
        choices=("jsonb", "columns"),
        help="Send the same mixed-type NDJSON to the regular JSONB or benchmark-only typed writer",
    )
    parser.add_argument("--label")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.upload_ids is not None and (
        len(args.upload_ids) != args.concurrency or len(set(args.upload_ids)) != args.concurrency
    ):
        parser.error("Provide exactly --concurrency distinct --upload-id values")
    if args.min_disk_free_gib < 1:
        parser.error("--min-disk-free-gib must be at least 1")
    if args.verify_counts_only and args.replay:
        parser.error("--verify-counts-only cannot be combined with --replay")
    if args.uvicorn_workers is not None and args.concurrency % args.uvicorn_workers:
        parser.error("--uvicorn-workers must divide --concurrency")
    result = asyncio.run(run(args))
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2), flush=True)


if __name__ == "__main__":
    main()
