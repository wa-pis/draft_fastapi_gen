"""Run against a dedicated PostgreSQL database via WRITER_TEST_DATABASE_URL."""

import asyncio
import hashlib
import json
import os
from contextlib import asynccontextmanager
from importlib.resources import files
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio

from async_postgres_writer.config import Settings
from async_postgres_writer.writer import Store, WriterError

pytestmark = pytest.mark.integration


async def chunks(*parts: bytes):
    for part in parts:
        yield part


@pytest_asyncio.fixture
async def database():
    dsn = os.getenv("WRITER_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Set WRITER_TEST_DATABASE_URL to a dedicated test database")
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4, command_timeout=10)
    schema = files("async_postgres_writer").joinpath("schema.sql").read_text()
    async with pool.acquire() as connection, connection.transaction():
        await connection.execute(schema)
    settings = Settings(
        database_url=dsn,
        api_token="integration-test-token-32-characters",
        acquire_timeout_seconds=0.1,
        upload_timeout_seconds=5,
        read_timeout_seconds=2,
    )
    try:
        yield Store(settings, pool), pool
    finally:
        await pool.close()


@pytest_asyncio.fixture
async def upload_id(database):
    _, pool = database
    identifier = uuid4()
    try:
        yield identifier
    finally:
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute(
                "DELETE FROM stream_writer.rows WHERE upload_id=$1", identifier
            )
            await connection.execute(
                "DELETE FROM stream_writer.uploads WHERE upload_id=$1", identifier
            )


async def row_count(pool, identifier):
    return await pool.fetchval(
        "SELECT count(*) FROM stream_writer.rows WHERE upload_id=$1", identifier
    )


async def test_copy_commits_rows_and_receipt_and_replay_is_idempotent(database, upload_id):
    store, pool = database
    payload = '{"id":1,"name":"Тест"}\n{"id":2,"nested":[null,true]}'.encode()
    receipt = await store.write(upload_id, chunks(payload[:7], payload[7:]))
    assert receipt.upload_id == upload_id
    assert receipt.row_count == 2
    assert receipt.byte_count == len(payload)
    assert receipt.sha256 == hashlib.sha256(payload).hexdigest()
    assert receipt.committed_at.tzinfo is not None
    assert receipt.reused is False

    saved = await store.lookup(upload_id)
    assert saved.sha256 == receipt.sha256
    assert saved.committed_at == receipt.committed_at
    rows = await pool.fetch(
        "SELECT row_number, data FROM stream_writer.rows WHERE upload_id=$1 ORDER BY row_number",
        upload_id,
    )
    assert [(row["row_number"], json.loads(row["data"])) for row in rows] == [
        (1, {"id": 1, "name": "Тест"}),
        (2, {"id": 2, "nested": [None, True]}),
    ]
    repeated = await store.write(upload_id, chunks(payload))
    assert repeated.reused is True
    assert repeated.committed_at == receipt.committed_at
    assert await row_count(pool, upload_id) == 2


async def test_same_id_with_different_bytes_conflicts_without_changing_data(database, upload_id):
    store, pool = database
    first = await store.write(upload_id, chunks(b'{"value":1}\n'))
    with pytest.raises(WriterError) as error:
        await store.write(upload_id, chunks(b'{"value":2}\n'))
    assert error.value.status_code == 409
    assert (await store.lookup(upload_id)).sha256 == first.sha256
    assert await row_count(pool, upload_id) == 1


async def test_formatting_changes_count_as_different_payload(database, upload_id):
    store, pool = database
    await store.write(upload_id, chunks(b'{"value":1}\n'))
    with pytest.raises(WriterError) as error:
        await store.write(upload_id, chunks(b'{ "value": 1 }\n'))
    assert error.value.status_code == 409
    assert await row_count(pool, upload_id) == 1


async def test_late_bad_row_rolls_back_previous_copy_rows_and_allows_retry(database, upload_id):
    store, pool = database

    async def invalid_late():
        for _ in range(3000):
            yield b'{"padding":"' + b"x" * 128 + b'"}\n'
        yield b'{"broken":}\n'

    with pytest.raises(WriterError) as error:
        await store.write(upload_id, invalid_late())
    assert error.value.status_code == 400
    assert await store.lookup(upload_id) is None
    assert await row_count(pool, upload_id) == 0
    receipt = await store.write(upload_id, chunks(b'{"valid":true}\n'))
    assert receipt.row_count == 1


async def test_source_disconnect_rolls_back_copy(database, upload_id):
    store, pool = database

    async def disconnected():
        yield b'{"ok":true}\n'
        raise ConnectionError("input disconnected")

    with pytest.raises((WriterError, ConnectionError)):
        await store.write(upload_id, disconnected())
    assert await store.lookup(upload_id) is None
    assert await row_count(pool, upload_id) == 0
    await store.check()


async def test_cancellation_rolls_back_and_releases_connection(database, upload_id):
    store, pool = database
    receiving = asyncio.Event()
    finish = asyncio.Event()

    async def waiting():
        yield b'{"ok":true}\n'
        receiving.set()
        await finish.wait()
        yield b'{"ok":true}\n'

    task = asyncio.create_task(store.write(upload_id, waiting()))
    try:
        await asyncio.wait_for(receiving.wait(), timeout=2)
        assert await store.lookup(upload_id) is None
        assert await row_count(pool, upload_id) == 0
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await store.lookup(upload_id) is None
        assert await row_count(pool, upload_id) == 0
        await store.check()
    finally:
        finish.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_concurrent_same_id_is_rejected_and_receipt_stays_invisible_until_commit(
    database, upload_id
):
    store, pool = database
    receiving = asyncio.Event()
    finish = asyncio.Event()

    async def waiting():
        yield b'{"part":1}\n'
        receiving.set()
        await finish.wait()
        yield b'{"part":2}\n'

    first = asyncio.create_task(store.write(upload_id, waiting()))
    try:
        await asyncio.wait_for(receiving.wait(), timeout=2)
        assert await store.lookup(upload_id) is None
        assert await row_count(pool, upload_id) == 0
        with pytest.raises(WriterError) as error:
            await store.write(upload_id, chunks(b'{"part":3}\n'))
        assert error.value.status_code == 409
        finish.set()
        receipt = await first
        assert receipt.row_count == 2
        assert await row_count(pool, upload_id) == 2
        assert (await store.lookup(upload_id)).sha256 == receipt.sha256
    finally:
        finish.set()
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)


@pytest.mark.parametrize("bad_row", [b'{"bad":"\\u0000"}\n', b'{"bad":1e200000}\n'])
async def test_jsonb_rejection_does_not_leave_rows_or_receipt(database, upload_id, bad_row):
    store, pool = database
    with pytest.raises(WriterError) as error:
        await store.write(upload_id, chunks(b'{"good":1}\n', bad_row))
    assert error.value.status_code == 400
    assert await store.lookup(upload_id) is None
    assert await row_count(pool, upload_id) == 0


async def test_pool_exhaustion_is_bounded_and_does_not_consume_body(database, upload_id):
    store, pool = database
    held = [await pool.acquire() for _ in range(pool.get_max_size())]
    consumed = False

    async def body():
        nonlocal consumed
        consumed = True
        yield b"{}\n"

    try:
        with pytest.raises(WriterError) as error:
            await asyncio.wait_for(store.write(upload_id, body()), timeout=1)
        assert error.value.status_code == 503
        assert consumed is False
    finally:
        for connection in held:
            await pool.release(connection)
    assert await store.lookup(upload_id) is None


async def test_overall_upload_timeout_rolls_back(database, upload_id):
    store, pool = database
    settings = Settings(
        database_url=os.environ["WRITER_TEST_DATABASE_URL"],
        api_token="integration-test-token-32-characters",
        upload_timeout_seconds=0.05,
        read_timeout_seconds=1,
    )
    limited_store = Store(settings, pool)
    never = asyncio.Event()

    async def slow_body():
        yield b'{"good":1}\n'
        await never.wait()
        yield b'{"good":2}\n'

    with pytest.raises(WriterError) as error:
        await limited_store.write(upload_id, slow_body())
    assert error.value.status_code == 408
    assert await store.lookup(upload_id) is None
    assert await row_count(pool, upload_id) == 0


async def test_lost_commit_response_is_recoverable_without_duplicate_rows(database, upload_id):
    store, pool = database

    class ConnectionProxy:
        def __init__(self, connection, owner):
            self.connection = connection
            self.owner = owner

        def __getattr__(self, name):
            return getattr(self.connection, name)

        @asynccontextmanager
        async def transaction(self):
            async with self.connection.transaction():
                yield
            if self.owner.lose_response:
                self.owner.lose_response = False
                raise asyncpg.ConnectionDoesNotExistError("Simulated lost COMMIT response")

    class PoolProxy:
        lose_response = True

        @asynccontextmanager
        async def acquire(self, **kwargs):
            async with pool.acquire(**kwargs) as connection:
                yield ConnectionProxy(connection, self)

    faulted = Store(store.settings, PoolProxy())
    payload = b'{"value":1}\n'
    with pytest.raises(WriterError) as error:
        await faulted.write(upload_id, chunks(payload))
    assert error.value.status_code == 503
    assert error.value.code == "commit_unknown"
    saved = await store.lookup(upload_id)
    assert saved is not None
    assert saved.sha256 == hashlib.sha256(payload).hexdigest()
    assert await row_count(pool, upload_id) == 1
    replayed = await faulted.write(upload_id, chunks(payload))
    assert replayed.reused is True
    assert replayed.committed_at == saved.committed_at
    assert await row_count(pool, upload_id) == 1
