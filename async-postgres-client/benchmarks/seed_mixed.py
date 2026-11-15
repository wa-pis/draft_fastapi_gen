"""Seed mixed-width read benchmarks in an initialized, empty PostgreSQL test database."""

import argparse
import asyncio
import hashlib
import json
import os
from base64 import b64encode
from collections import Counter
from collections.abc import Iterator
from itertools import repeat
from pathlib import Path
from uuid import UUID

import asyncpg

_NARROW_BYTES = 220
_WIDE_BYTES = (768 * 1024, 256 * 1024, 256 * 1024, 256 * 1024)
_WIDE_ROWS = 4096
_BLOCK_ROWS = 64
_PROFILE_IDS = {"alternating": 1001, "bursts": 1002}


def payload_widths(profile: str) -> Iterator[int]:
    for index in range(_WIDE_ROWS):
        if profile == "alternating":
            yield _NARROW_BYTES
        elif index % _BLOCK_ROWS == 0:
            # Start with 65 narrow rows so the adaptive fetch after 1+32+32 hits wide rows.
            count = _BLOCK_ROWS + (index == 0) - (index + _BLOCK_ROWS == _WIDE_ROWS)
            yield from repeat(_NARROW_BYTES, count)
        yield _WIDE_BYTES[index % len(_WIDE_BYTES)]


async def seed_profile(conn: asyncpg.Connection, profile: str) -> dict:
    upload_id = UUID(f"90000000-0000-4000-8000-{_PROFILE_IDS[profile]:012d}")
    count = 2 * _WIDE_ROWS
    await conn.execute(
        "INSERT INTO stream_writer.uploads (upload_id, sha256, row_count, byte_count) "
        "VALUES ($1, repeat('0', 64), $2, 0)",
        upload_id,
        count,
    )
    digest = hashlib.sha256()
    payload_counts: Counter[int] = Counter()
    stream_bytes = 0

    def records() -> Iterator[tuple[UUID, int, str]]:
        nonlocal stream_bytes
        for row_number, width in enumerate(payload_widths(profile), start=1):
            payload_counts[width] += 1
            identity = width.to_bytes(4, "big") + payload_counts[width].to_bytes(4, "big")
            payload = b64encode(hashlib.shake_256(identity).digest(width * 3 // 4)).decode("ascii")
            data = f'{{"payload": "{payload}"}}'
            # PostgreSQL JSONB renders "data" before the longer "row_number" key.
            line = f'{{"data": {data}, "row_number": {row_number}}}\n'.encode("ascii")
            digest.update(line)
            stream_bytes += len(line)
            yield upload_id, row_number, data

    status = await conn.copy_records_to_table(
        "rows",
        schema_name="stream_writer",
        columns=("upload_id", "row_number", "data"),
        records=records(),
    )
    expected_counts = Counter(_WIDE_BYTES * (_WIDE_ROWS // len(_WIDE_BYTES)))
    expected_counts[_NARROW_BYTES] = _WIDE_ROWS
    if status != f"COPY {count}" or payload_counts != expected_counts:
        raise RuntimeError(f"Incorrect {profile} fixture: {status}, widths={dict(payload_counts)}")
    return {
        "upload_id": str(upload_id),
        "payload_width_counts": dict(payload_counts),
        "stream": {"rows": count, "bytes": stream_bytes, "sha256": digest.hexdigest()},
    }


async def seed(dsn: str) -> dict:
    profiles = {}
    conn = await asyncpg.connect(dsn)
    try:
        async with conn.transaction():
            await conn.execute(
                "LOCK TABLE stream_writer.uploads, stream_writer.rows IN ACCESS EXCLUSIVE MODE NOWAIT"
            )
            if await conn.fetchval("SELECT EXISTS (SELECT FROM stream_writer.uploads)"):
                raise RuntimeError("Mixed seed requires an initialized, empty test database")
            await conn.execute("SET LOCAL synchronous_commit=on")
            for profile in _PROFILE_IDS:
                profiles[profile] = await seed_profile(conn, profile)
        await conn.execute("ANALYZE stream_writer.rows")
        return {
            "fixture": "Identical Base64(SHAKE-256(width, width ordinal)) payloads in two orders",
            "fixture_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "postgres_version": await conn.fetchval("SHOW server_version"),
            "rows_relation_bytes": await conn.fetchval(
                "SELECT pg_total_relation_size('stream_writer.rows')"
            ),
            "toast_relation_bytes": await conn.fetchval(
                "SELECT pg_total_relation_size(reltoastrelid) FROM pg_class "
                "WHERE oid = 'stream_writer.rows'::regclass"
            ),
            "profiles": profiles,
        }
    finally:
        await conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    if not (dsn := os.getenv("WRITER_TEST_DATABASE_URL")):
        parser.error("Set WRITER_TEST_DATABASE_URL for an initialized, empty test database")
    print(json.dumps(asyncio.run(seed(dsn)), indent=2))


if __name__ == "__main__":
    main()
