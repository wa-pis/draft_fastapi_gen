"""Exercise HTTP result reads against a dedicated PostgreSQL database."""

import asyncio
import json
import os
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio

from async_postgres_writer import writer
from async_postgres_writer.__main__ import init_db
from async_postgres_writer.app import create_app
from async_postgres_writer.config import Settings

pytestmark = pytest.mark.integration

TOKEN = "integration-test-token-32-characters"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
NDJSON = {**AUTH, "Content-Type": "application/x-ndjson"}


@pytest_asyncio.fixture
async def read_api():
    dsn = os.getenv("WRITER_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Set WRITER_TEST_DATABASE_URL to a dedicated test database")

    settings = Settings(database_url=dsn, api_token=TOKEN, pool_size=2, upload_timeout_seconds=10)
    await init_db(settings)
    app = create_app(settings)
    upload_id, other_id = uuid4(), uuid4()
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        try:
            yield client, app, upload_id, other_id
        finally:
            async with app.state.store.pool.acquire() as connection, connection.transaction():
                await connection.execute(
                    "DELETE FROM stream_writer.rows WHERE upload_id IN ($1, $2)",
                    upload_id,
                    other_id,
                )
                await connection.execute(
                    "DELETE FROM stream_writer.uploads WHERE upload_id IN ($1, $2)",
                    upload_id,
                    other_id,
                )


def decode_json(raw):
    return json.loads(raw, parse_float=Decimal, parse_int=Decimal)


async def test_pages_and_ndjson_export_match_committed_rows_in_order(read_api):
    client, app, upload_id, other_id = read_api
    upload_path = f"/v1/uploads/{upload_id}"
    high_precision = Decimal("123.4567890123456789012345")
    rows = [{"index": index, "text": 'Привет\n"мир"'} for index in range(1, 131)]
    # Export row 32 is last in the first 32-row fetch and spans multiple HTTP chunks.
    rows[30]["text"] += "я" * (64 * 1024)
    numeric_row = b'{"decimal":123.4567890123456789012345,"exponent":1e400}\n'
    body = numeric_row + b"".join(
        json.dumps(row, ensure_ascii=False).encode() + b"\n" for row in rows
    )
    expected = [
        {"row_number": number, "data": row}
        for number, row in enumerate(
            [{"decimal": high_precision, "exponent": Decimal("1e400")}, *rows], start=1
        )
    ]
    created = await client.put(upload_path, headers=NDJSON, content=body)
    assert created.status_code == 201
    assert created.json()["row_count"] == len(expected)
    other = await client.put(f"/v1/uploads/{other_id}", headers=NDJSON, content=b'{"other":true}\n')
    assert other.status_code == 201

    first = await client.get(f"{upload_path}/rows", headers=AUTH, params={"limit": 3, "offset": 0})
    assert first.status_code == 200
    assert first.headers["Content-Type"].split(";")[0] == "application/json"
    assert decode_json(first.content) == {
        "upload_id": str(upload_id),
        "row_count": len(expected),
        "limit": 3,
        "offset": 0,
        "rows": expected[:3],
    }
    first_by_key = await client.get(
        f"{upload_path}/rows", headers=AUTH, params={"limit": 3, "after_row_number": 0}
    )
    assert first_by_key.status_code == 200
    assert decode_json(first_by_key.content) == {
        "upload_id": str(upload_id),
        "row_count": len(expected),
        "limit": 3,
        "after_row_number": 0,
        "next_after_row_number": 3,
        "rows": expected[:3],
    }
    second = await client.get(f"{upload_path}/rows", headers=AUTH, params={"limit": 3, "offset": 3})
    assert second.status_code == 200
    assert second.json()["rows"] == expected[3:6]
    past_end = await client.get(
        f"{upload_path}/rows", headers=AUTH, params={"offset": len(expected) + 100}
    )
    assert past_end.status_code == 200
    assert past_end.json()["row_count"] == len(expected)
    assert past_end.json()["rows"] == []

    app.state.store.settings.export_prefetch_rows = 8
    app.state.store.settings.export_prefetch_adaptive = False
    baseline = await client.get(f"{upload_path}/rows/stream", headers=AUTH)
    assert baseline.status_code == 200
    assert baseline.headers["Content-Type"].split(";")[0] == "application/x-ndjson"
    assert baseline.headers["X-Row-Count"] == str(len(expected))
    assert baseline.content.endswith(b"\n")
    assert [decode_json(line) for line in baseline.content.splitlines()] == expected

    for prefetch, adaptive in ((32, False), (8, True)):
        app.state.store.settings.export_prefetch_rows = prefetch
        app.state.store.settings.export_prefetch_adaptive = adaptive
        tuned = await client.get(f"{upload_path}/rows/stream", headers=AUTH)
        assert tuned.status_code == 200
        assert tuned.headers == baseline.headers
        assert tuned.content == baseline.content


async def test_keyset_handles_row_number_gaps_and_an_exact_final_page(read_api):
    client, app, upload_id, _ = read_api
    upload_path = f"/v1/uploads/{upload_id}"
    uploaded = await client.put(upload_path, headers=NDJSON, content=b"{}\n" * 4)
    assert uploaded.status_code == 201
    async with app.state.store.pool.acquire() as conn:
        await conn.execute(
            "UPDATE stream_writer.rows SET row_number = row_number + 2 "
            "WHERE upload_id = $1 AND row_number >= 3",
            upload_id,
        )

    async def page(after):
        response = await client.get(
            f"{upload_path}/rows", headers=AUTH, params={"limit": 2, "after_row_number": after}
        )
        assert response.status_code == 200
        return response.json()

    first = await page(0)
    assert [row["row_number"] for row in first["rows"]] == [1, 2]
    assert first["next_after_row_number"] == 2
    last = await page(first["next_after_row_number"])
    assert [row["row_number"] for row in last["rows"]] == [5, 6]
    assert last["next_after_row_number"] is None
    assert (await page(5))["rows"] == [{"row_number": 6, "data": {}}]
    past_end = await page(2**63 - 1)
    assert past_end["rows"] == [] and past_end["next_after_row_number"] is None

    conflict = await client.get(
        f"{upload_path}/rows", headers=AUTH, params={"offset": 0, "after_row_number": 0}
    )
    assert conflict.status_code == 422


async def test_hard_prefetch_caps_pg_results_and_preserves_oversized_unicode(read_api, monkeypatch):
    client, app, upload_id, _ = read_api
    budget = 2 * 1024**2

    def line(number, payload):
        return (
            json.dumps(
                {"data": {"payload": payload}, "row_number": number}, ensure_ascii=False
            ).encode()
            + b"\n"
        )

    oversized = "🌊" * (budget // 4 + 1)
    if line(70, oversized)[budget] & 0xC0 != 0x80:
        oversized = "x" + oversized
    oversized_line = line(70, oversized)
    assert oversized_line[budget] & 0xC0 == 0x80

    rows = [
        *((number, "") for number in range(1, 66)),
        *((number, "x" * (768 * 1024)) for number in (66, 67, 68)),
        (69, "x" * (budget - len(line(69, "")))),
        (70, oversized),
        (72, "after the gap"),
    ]
    async with app.state.store.pool.acquire() as conn, conn.transaction():
        await conn.execute(
            "INSERT INTO stream_writer.uploads (upload_id, sha256, row_count, byte_count) "
            "VALUES ($1, repeat('0', 64), $2, 0)",
            upload_id,
            len(rows),
        )
        await conn.copy_records_to_table(
            "rows",
            schema_name="stream_writer",
            columns=("upload_id", "row_number", "data"),
            records=(
                (upload_id, number, json.dumps({"payload": payload}, ensure_ascii=False))
                for number, payload in rows
            ),
        )

    batches = []
    slices = []
    original = writer._byte_limited_export_rows

    class ProbedConnection:
        def __init__(self, conn):
            self.conn = conn

        async def fetch(self, *args):
            result = await self.conn.fetch(*args)
            size = sum(len(row["body"]) for row in result)
            assert size <= budget
            batches.append(([row["row_number"] for row in result], size))
            return result

        async def fetchval(self, *args):
            result = await self.conn.fetchval(*args)
            assert 0 < len(result) <= budget
            slices.append(len(result))
            return result

    monkeypatch.setattr(
        writer, "_byte_limited_export_rows", lambda conn, id: original(ProbedConnection(conn), id)
    )
    settings = app.state.store.settings
    path = f"/v1/uploads/{upload_id}/rows/stream"
    expected = b"".join(line(number, payload) for number, payload in rows)
    for count, hard_limit in ((8, False), (32, False), (8, True)):
        settings.export_prefetch_rows = count
        settings.export_prefetch_hard_limit = hard_limit
        response = await client.get(path, headers=AUTH)
        assert response.status_code == 200
        assert response.headers["X-Row-Count"] == str(len(rows))
        assert response.content == expected
    assert [numbers for numbers, _ in batches] == [
        [1],
        list(range(2, 34)),
        list(range(34, 66)),
        [66, 67],
        [68],
        [69],
        [70],
        [72],
        [],
    ]
    assert batches[5:7] == [([69], budget), ([70], budget)]
    assert slices == [len(oversized_line) - budget]


async def test_result_reads_hide_in_progress_and_rolled_back_upload(read_api):
    client, _, upload_id, _ = read_api
    upload_path = f"/v1/uploads/{upload_id}"
    waiting, finish = asyncio.Event(), asyncio.Event()

    async def input_stream():
        yield b'{"before":"rollback"}\n'
        waiting.set()
        await finish.wait()
        yield b'{"broken":}\n'

    put = asyncio.create_task(client.put(upload_path, headers=NDJSON, content=input_stream()))
    async with asyncio.timeout(5):
        try:
            await waiting.wait()
            for suffix in ("/rows", "/rows/stream"):
                response = await client.get(upload_path + suffix, headers=AUTH)
                assert response.status_code == 404
        finally:
            finish.set()
            rejected = await put

    assert rejected.status_code == 400
    for suffix in ("/rows", "/rows/stream"):
        response = await client.get(upload_path + suffix, headers=AUTH)
        assert response.status_code == 404
