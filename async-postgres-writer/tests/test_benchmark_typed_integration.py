"""Small end-to-end check for the benchmark JSONB and typed HTTP writers."""

import hashlib
import json
import os
import sys
from contextlib import AsyncExitStack
from importlib import import_module
from importlib.resources import files
from pathlib import Path
from random import Random
from uuid import uuid4

import asyncpg
import httpx
import pytest
import pytest_asyncio

from async_postgres_writer.app import create_app
from async_postgres_writer.config import Settings

pytestmark = pytest.mark.integration

TOKEN = "integration-test-token-32-characters"
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/x-ndjson",
}
ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT))
typed_data = import_module("benchmarks.typed_data")
typed_writer = import_module("benchmarks.typed_writer")
TYPED_COLUMNS = typed_data.TYPED_COLUMNS
typed_record = typed_data.typed_record
typed_row_bytes = typed_data.typed_row_bytes
create_typed_app = typed_writer.create_typed_app


@pytest_asyncio.fixture
async def benchmark_apps():
    dsn = os.getenv("WRITER_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Set WRITER_TEST_DATABASE_URL to a dedicated test database")

    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4, command_timeout=10)
    identifiers = [uuid4() for _ in range(3)]
    initialized = False
    try:
        async with pool.acquire() as connection:
            await connection.execute(
                files("async_postgres_writer").joinpath("schema.sql").read_text()
            )
            await connection.execute((ROOT / "benchmarks/typed_rows.sql").read_text())
            has_fk = await connection.fetchval(
                "SELECT EXISTS (SELECT FROM pg_constraint "
                "WHERE conrelid='stream_writer_bench.typed_rows'::regclass "
                "AND conname='typed_rows_upload_id_fkey')"
            )
            if not has_fk:
                await connection.execute((ROOT / "benchmarks/typed_rows_fk.sql").read_text())
        initialized = True

        settings = Settings(
            database_url=dsn,
            api_token=TOKEN,
            max_upload_bytes=1024**2,
            max_line_bytes=8192,
            acquire_timeout_seconds=1,
            upload_timeout_seconds=10,
            read_timeout_seconds=2,
        )
        jsonb_app = create_app(settings)
        typed_app = create_typed_app(settings)
        async with AsyncExitStack() as stack:
            await stack.enter_async_context(jsonb_app.router.lifespan_context(jsonb_app))
            await stack.enter_async_context(typed_app.router.lifespan_context(typed_app))
            yield pool, jsonb_app, typed_app, identifiers
    finally:
        if initialized:
            async with pool.acquire() as connection, connection.transaction():
                await connection.execute(
                    "DELETE FROM stream_writer_bench.typed_rows WHERE upload_id=ANY($1::uuid[])",
                    identifiers,
                )
                await connection.execute(
                    "DELETE FROM stream_writer.rows WHERE upload_id=ANY($1::uuid[])", identifiers
                )
                await connection.execute(
                    "DELETE FROM stream_writer.uploads WHERE upload_id=ANY($1::uuid[])",
                    identifiers,
                )
        await pool.close()


async def test_jsonb_and_typed_full_readback_replay_and_typed_rollback(benchmark_apps):
    pool, jsonb_app, typed_app, identifiers = benchmark_apps
    jsonb_id, typed_id, failed_id = identifiers
    generator = Random(20261108)
    lines = [typed_row_bytes(number, generator) for number in range(1, 6)]
    body = b"".join(lines)
    digest = hashlib.sha256(body).hexdigest()

    async with (
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=jsonb_app), base_url="http://jsonb"
        ) as jsonb_client,
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=typed_app), base_url="http://typed"
        ) as typed_client,
    ):
        assert (await jsonb_client.get("/health/ready", headers=HEADERS)).status_code == 200
        assert (await typed_client.get("/health/ready", headers=HEADERS)).status_code == 200

        jsonb_response = await jsonb_client.put(
            f"/v1/uploads/{jsonb_id}", headers=HEADERS, content=body
        )
        typed_response = await typed_client.put(
            f"/v1/uploads/{typed_id}", headers=HEADERS, content=body
        )
        for response, identifier in (
            (jsonb_response, jsonb_id),
            (typed_response, typed_id),
        ):
            assert response.status_code == 201, response.text
            receipt = response.json()
            assert receipt["upload_id"] == str(identifier)
            assert receipt["row_count"] == len(lines)
            assert receipt["byte_count"] == len(body)
            assert receipt["sha256"] == digest
            assert receipt["reused"] is False

        jsonb_rows = await pool.fetch(
            "SELECT row_number, data FROM stream_writer.rows "
            "WHERE upload_id=$1 ORDER BY row_number",
            jsonb_id,
        )
        typed_rows = await pool.fetch(
            "SELECT * FROM stream_writer_bench.typed_rows WHERE upload_id=$1 ORDER BY row_number",
            typed_id,
        )
        assert len(jsonb_rows) == len(typed_rows) == len(lines)
        for number, (line, jsonb_row, typed_row) in enumerate(
            zip(lines, jsonb_rows, typed_rows, strict=True), start=1
        ):
            assert jsonb_row["row_number"] == number
            assert json.loads(jsonb_row["data"]) == json.loads(line)
            expected = typed_record(typed_id, number, line.decode())
            for column, value in zip(TYPED_COLUMNS, expected, strict=True):
                stored = typed_row[column]
                if column == "metrics" and value is not None:
                    assert json.loads(stored) == json.loads(value)
                else:
                    assert stored == value, (number, column)

        for client, identifier, original in (
            (jsonb_client, jsonb_id, jsonb_response.json()),
            (typed_client, typed_id, typed_response.json()),
        ):
            replay = await client.put(f"/v1/uploads/{identifier}", headers=HEADERS, content=body)
            assert replay.status_code == 200
            assert replay.json() == {**original, "reused": True}

        assert await pool.fetchval(
            "SELECT count(*) FROM stream_writer.rows WHERE upload_id=$1", jsonb_id
        ) == len(lines)
        assert await pool.fetchval(
            "SELECT count(*) FROM stream_writer_bench.typed_rows WHERE upload_id=$1", typed_id
        ) == len(lines)

        failed_row = json.loads(lines[1])
        failed_row["account_code"] = "x" * 33
        failed_body = lines[0] + json.dumps(failed_row, separators=(",", ":")).encode() + b"\n"
        failed = await typed_client.put(
            f"/v1/uploads/{failed_id}", headers=HEADERS, content=failed_body
        )
        assert failed.status_code == 400
        assert failed.json()["error"] == "invalid_stream"
        assert (
            await pool.fetchval(
                "SELECT count(*) FROM stream_writer_bench.typed_rows WHERE upload_id=$1",
                failed_id,
            )
            == 0
        )
        assert (
            await pool.fetchval(
                "SELECT count(*) FROM stream_writer.uploads WHERE upload_id=$1", failed_id
            )
            == 0
        )
        assert (
            await typed_client.get(f"/v1/uploads/{failed_id}", headers=HEADERS)
        ).status_code == 404
