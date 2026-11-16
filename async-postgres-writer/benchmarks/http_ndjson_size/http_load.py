"""Measure chunked HTTP PUT of mixed JSONB on an isolated disposable Compose project."""

import argparse
import asyncio
import json
import os
import platform
import resource
import shutil
from contextlib import suppress
from pathlib import Path
from time import monotonic
from uuid import UUID

import httpx
from mixed_fixture import MixedFixture
from stack import DIR, api_url, compose, inspect_project, psql, wait_ready

MIB = 1024**2
GIB = 1024**3
POSTGRES = """
SELECT json_build_object(
    'version', current_setting('server_version'),
    'wal_buffers_bytes', pg_size_bytes(current_setting('wal_buffers')),
    'max_wal_size', current_setting('max_wal_size'),
    'shared_buffers', current_setting('shared_buffers'),
    'checkpoint_timeout', current_setting('checkpoint_timeout'),
    'fsync', current_setting('fsync'),
    'synchronous_commit', current_setting('synchronous_commit'),
    'full_page_writes', current_setting('full_page_writes'),
    'track_io_timing', current_setting('track_io_timing'),
    'track_wal_io_timing', current_setting('track_wal_io_timing'),
    'role_connection_limit', (SELECT rolconnlimit FROM pg_roles WHERE rolname='benchmark_writer'),
    'role_superuser', (SELECT rolsuper FROM pg_roles WHERE rolname='benchmark_writer'),
    'trigger_state', (SELECT tgenabled FROM pg_trigger
      WHERE tgrelid='stream_writer.rows'::regclass AND tgname='rows_set_ndjson_bytes'),
    'logged_tables', (
      SELECT bool_and(relpersistence='p') FROM pg_class WHERE oid IN (
        'stream_writer.rows'::regclass, 'stream_writer.uploads'::regclass,
        (SELECT reltoastrelid FROM pg_class WHERE oid='stream_writer.rows'::regclass))))
"""
COUNTERS = """
SELECT json_build_object(
    'wal_lsn', pg_current_wal_insert_lsn() - '0/0'::pg_lsn,
    'wal_buffers_full', wal_buffers_full,
    'timed_checkpoints', num_timed,
    'requested_checkpoints', num_requested,
    'checkpoint_write_ms', write_time,
    'checkpoint_sync_ms', sync_time)
FROM pg_stat_wal CROSS JOIN pg_stat_checkpointer
"""
VERIFY = """
SELECT json_build_object(
    'receipt', to_jsonb(u) - 'committed_at',
    'rows', count(*), 'min_row', min(r.row_number), 'max_row', max(r.row_number),
    'present_sizes', count(r.ndjson_bytes), 'total_sizes', sum(r.ndjson_bytes),
    'invalid_sizes', count(*) FILTER (WHERE r.ndjson_bytes IS NOT NULL
        AND r.ndjson_bytes <> octet_length(payload) + length(r.row_number::text) + 42),
    'payload_signature', encode(sha256(convert_to(
        string_agg(encode(sha256(convert_to(payload, 'UTF8')), 'hex'), '' ORDER BY r.row_number),
        'UTF8')), 'hex'))
FROM stream_writer.uploads u JOIN stream_writer.rows r USING (upload_id)
CROSS JOIN LATERAL (SELECT r.data->>'payload' AS payload OFFSET 0) AS value
WHERE u.upload_id = '{upload_id}' GROUP BY u.upload_id
"""
METRICS = r"""set -eu
cd /sys/fs/cgroup
awk '/^usage_usec / {print $2}' cpu.stat
awk '/^anon / {print $2}' memory.stat
cat memory.current
if [ -n "${PGDATA:-}" ]; then
  for entry in /proc/[0-9]*/cmdline; do
    if [ -r "$entry" ]; then tr '\000' '\n' < "$entry" 2>/dev/null | sed -n '1p' || true; fi
  done | awk '/^postgres: benchmark_writer stream_writer .*COPY/ {copy++}
      /^postgres: benchmark_writer stream_writer / {app++} END {print copy+0; print app+0;}'
  df -Pk "$PGDATA" | awk 'END {print $4}'
else
  awk '/^VmRSS:/ {print $2}' /proc/1/status
fi
"""


def metrics(service):
    usage, anon, memory, *extra = map(
        int, compose("exec", "-T", service, "sh", "-c", METRICS, timeout=15).splitlines()
    )
    result = dict(cpu_usec=usage, anon_bytes=anon, memory_bytes=memory)
    if service == "postgres":
        active, connections, guest_kib = extra
        result.update(
            copy_backends=active, app_connections=connections, free_bytes=guest_kib * 1024
        )
    else:
        result["rss_bytes"] = extra[0] * 1024
    return result


async def snapshot():
    pg, api = await asyncio.gather(
        asyncio.to_thread(metrics, "postgres"), asyncio.to_thread(metrics, "writer")
    )
    return dict(postgres=pg, api=api, host_free_bytes=shutil.disk_usage(DIR).free)


async def sample(stop, samples, started):
    while not stop.is_set():
        snap = await snapshot()
        snap["at_s"] = monotonic() - started
        if not stop.is_set():
            samples.append(snap)
        if min(snap["postgres"]["free_bytes"], snap["host_free_bytes"]) < 10 * GIB:
            raise RuntimeError("Benchmark stopped to preserve 10 GiB disk reserve.")
        with suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), 0.85)


async def put(client, upload_id, fixture, expected, counts, index):
    async def body():
        for chunk in fixture.chunks():
            counts[index] += len(chunk)
            yield chunk

    start = monotonic()
    response = await client.put(f"/v1/uploads/{upload_id}", content=body())
    finished = monotonic()
    if response.status_code != 201:
        raise RuntimeError(f"PUT returned {response.status_code}: {response.text[:300]}")
    receipt = response.json()
    assert receipt["upload_id"] == str(upload_id)
    assert receipt["sha256"] == expected["sha256"] and receipt["row_count"] == expected["rows"]
    assert receipt["byte_count"] == expected["bytes"] and receipt["reused"] is False
    return dict(receipt=receipt, seconds=finished - start), finished


async def measure(args, fixture, expected, ids, url):
    counts = [0] * len(ids)
    samples = []
    stop = asyncio.Event()
    headers = {
        "Authorization": f"Bearer {os.environ['WRITER_API_TOKEN']}",
        "Content-Type": "application/x-ndjson",
    }
    async with httpx.AsyncClient(
        base_url=url, headers=headers, timeout=3600, trust_env=False
    ) as client:
        response = await client.get("/health/ready")
        response.raise_for_status()
        before = await snapshot()
        if (
            min(before["postgres"]["free_bytes"], before["host_free_bytes"])
            < expected["bytes"] * len(ids) + 16 * GIB
        ):
            raise RuntimeError("Not enough free disk for committed data plus WAL and reserve.")
        counters_before = json.loads(psql(COUNTERS))
        client_cpu_start = os.times()
        started = monotonic()
        async with asyncio.TaskGroup() as group:
            monitor = group.create_task(sample(stop, samples, started))
            tasks = [
                group.create_task(put(client, upload_id, fixture, expected, counts, i))
                for i, upload_id in enumerate(ids)
            ]
            try:
                uploads = await asyncio.gather(*tasks)
                client_cpu_end = os.times()
                stop.set()
                after = await snapshot()
            finally:
                stop.set()
        assert monitor.done()
    assert counts == [expected["bytes"]] * len(ids)
    elapsed = max(finished for _, finished in uploads) - started
    counters_after = json.loads(psql(COUNTERS))
    if args.require_overlap:
        assert samples
    peaks = {}
    for service in ("api", "postgres"):
        peaks[service] = {
            key: max(snap[service][key] for snap in [before, *samples, after])
            for key in (
                "anon_bytes",
                "memory_bytes",
                "rss_bytes",
                "copy_backends",
                "app_connections",
            )
            if key in before[service]
        }
    assert peaks["postgres"]["app_connections"] <= 4
    assert args.require_overlap is False or peaks["postgres"]["copy_backends"] == len(ids)
    return {
        "seconds": elapsed,
        "aggregate_mib_per_second": sum(counts) / MIB / elapsed,
        "cpu_seconds": {
            service: (after[service]["cpu_usec"] - before[service]["cpu_usec"]) / 1_000_000
            for service in ("api", "postgres")
        }
        | {
            "http_client": client_cpu_end.user
            + client_cpu_end.system
            - client_cpu_start.user
            - client_cpu_start.system
        },
        "client_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        * (1024 if platform.system() == "Linux" else 1),
        "peaks": peaks,
        "resource_samples": len(samples),
        "free_min_bytes": {
            "host": min(snap["host_free_bytes"] for snap in [before, *samples, after]),
            "guest": min(snap["postgres"]["free_bytes"] for snap in [before, *samples, after]),
        },
        "wal_checkpoints": {
            key: counters_after[key] - counters_before[key] for key in counters_before
        },
        "uploads": [upload for upload, _ in uploads],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--streams", type=int, choices=(2, 4), required=True)
    parser.add_argument("--trigger", choices=("on", "off"), required=True)
    parser.add_argument("--size-bytes", type=int, default=5 * GIB)
    parser.add_argument("--require-overlap", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 15 <= args.size_bytes <= 5 * GIB:
        parser.error("--size-bytes must be between 15 bytes and 5 GiB")

    enabled = args.trigger == "on"
    state = "A" if enabled else "D"
    ids = [UUID(f"20000000-0000-4000-8000-{i:012d}") for i in range(1, args.streams + 1)]
    fixture = MixedFixture(args.size_bytes)
    expected = fixture.expected()
    signature = fixture.expected_payload_signature()
    export_bytes = fixture.expected_export_bytes()
    mode = "ENABLE ALWAYS" if enabled else "DISABLE"
    inspect_project("postgres", "writer")
    url = api_url()
    psql(f"""
        TRUNCATE stream_writer.rows, stream_writer.uploads;
        ALTER TABLE stream_writer.rows {mode} TRIGGER rows_set_ndjson_bytes;
        CHECKPOINT;
    """)
    settings = json.loads(psql(POSTGRES))
    assert settings["version"].startswith("17.")
    assert settings["trigger_state"] == state and settings["logged_tables"]
    assert all(settings[key] == "on" for key in ("fsync", "synchronous_commit", "full_page_writes"))
    assert settings["wal_buffers_bytes"] == 64 * MIB
    assert settings["role_connection_limit"] == 4 and settings["role_superuser"] is False
    print(
        f"{args.output.name}: PUT {args.streams} x {expected['bytes']} bytes, trigger={args.trigger}",
        flush=True,
    )
    result = asyncio.run(measure(args, fixture, expected, ids, url))
    print(
        f"{args.output.name}: {result['seconds']:.2f}s, {result['aggregate_mib_per_second']:.2f} MiB/s; verifying after restart",
        flush=True,
    )
    assert json.loads(psql(POSTGRES)) == settings
    inspect_project("postgres", "writer")
    compose("restart", "postgres")
    compose(
        "exec",
        "-T",
        "postgres",
        "sh",
        "-ec",
        "until pg_isready -U writer -d stream_writer; do sleep 1; done",
        timeout=90,
    )
    assert json.loads(psql(POSTGRES)) == settings
    assert int(psql("SELECT count(*) FROM stream_writer.uploads")) == len(ids)
    verification = []
    for upload_id, upload in zip(ids, result["uploads"], strict=True):
        query = "SET enable_presorted_aggregate = off;\n" + VERIFY.format(upload_id=upload_id)
        row = json.loads(psql(query).splitlines()[-1])
        assert row["receipt"] == {
            key: value
            for key, value in upload["receipt"].items()
            if key not in ("committed_at", "reused")
        }
        assert row["min_row"] == 1 and row["max_row"] == row["rows"] == expected["rows"]
        assert row["payload_signature"] == signature
        assert row["invalid_sizes"] == 0
        assert row["present_sizes"] == (expected["rows"] if enabled else 0)
        assert row["total_sizes"] == (export_bytes if enabled else None)
        verification.append(row)
    inspect_project("postgres", "writer")
    compose("restart", "writer")
    wait_ready()
    result.update(
        fixture=expected | {"payload_signature": signature, "export_bytes": export_bytes},
        verification_after_postgres_restart=verification,
        settings=settings,
        streams=args.streams,
        trigger=args.trigger,
    )
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(f"{args.output.name}: verified", flush=True)


if __name__ == "__main__":
    main()
