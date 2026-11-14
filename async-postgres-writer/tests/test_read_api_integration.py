"""Exercise HTTP result reads against a dedicated PostgreSQL database."""

import asyncio
import json
import os
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio

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
    client, _, upload_id, other_id = read_api
    upload_path = f"/v1/uploads/{upload_id}"
    high_precision = Decimal("123.4567890123456789012345")
    rows = [{"index": index, "text": 'Привет\n"мир"'} for index in range(1, 131)]
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

    async with client.stream("GET", f"{upload_path}/rows/stream", headers=AUTH) as exported:
        assert exported.status_code == 200
        assert exported.headers["Content-Type"].split(";")[0] == "application/x-ndjson"
        assert exported.headers["X-Row-Count"] == str(len(expected))
        assert [decode_json(line) async for line in exported.aiter_lines()] == expected


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
