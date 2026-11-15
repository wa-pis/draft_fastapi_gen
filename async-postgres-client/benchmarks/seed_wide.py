"""Seed the wide-read benchmark in an initialized, empty PostgreSQL test database."""

import argparse
import asyncio
import hashlib
import json
import os
from base64 import b64encode
from pathlib import Path
from uuid import UUID

import asyncpg

_PROFILES = ((256, 6144), (768, 2048))


async def seed_profile(conn: asyncpg.Connection, kib: int, count: int) -> dict:
    upload_id = UUID(f"90000000-0000-4000-8000-{kib:012d}")
    await conn.execute(
        "INSERT INTO stream_writer.uploads (upload_id, sha256, row_count, byte_count) "
        "VALUES ($1, repeat('0', 64), $2, 0)",
        upload_id,
        count,
    )
    digest = hashlib.sha256()
    stream_bytes = 0

    def records():
        nonlocal stream_bytes
        for row_number in range(1, count + 1):
            random = hashlib.shake_256(row_number.to_bytes(8, "big"))
            # Three input bytes produce four Base64 characters.
            payload = b64encode(random.digest(kib * 768)).decode("ascii")
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
    if status != f"COPY {count}":
        raise RuntimeError(f"Incomplete {kib} KiB seed: {status}")
    return {
        "upload_id": str(upload_id),
        "payload_bytes": kib * 1024,
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
                raise RuntimeError("Wide seed requires an initialized, empty test database")
            await conn.execute("SET LOCAL synchronous_commit=on")
            for kib, count in _PROFILES:
                profiles[kib] = await seed_profile(conn, kib, count)
        await conn.execute("ANALYZE stream_writer.rows")
        return {
            "fixture": "unique Base64(SHAKE-256(uint64be(row_number))) payloads via binary COPY",
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
