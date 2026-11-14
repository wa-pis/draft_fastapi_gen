import asyncio
from contextlib import asynccontextmanager
from uuid import UUID

import httpx
import pytest
from pydantic import ValidationError
from starlette.requests import ClientDisconnect

from async_postgres_writer.app import create_app
from async_postgres_writer.config import Settings
from async_postgres_writer.writer import Store, WriterError

TOKEN = "a" * 32
AUTH = {"Authorization": f"Bearer {TOKEN}"}
UPLOAD_ID = UUID(int=1)
ROWS = f"/v1/uploads/{UPLOAD_ID}/rows"


@pytest.fixture
def settings():
    return Settings(database_url="postgresql://localhost/test", api_token=TOKEN)


@pytest.fixture
def app(settings):
    app = create_app(settings)
    app.state.store = object()  # Auth and validation must run before accessing the store.
    return app


@pytest.mark.parametrize("path", [ROWS, f"{ROWS}/stream"])
async def test_read_routes_require_auth_before_accessing_the_store(app, path):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(path)

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"


@pytest.mark.parametrize(
    "query,parameter",
    [
        ("limit=0", "limit"),
        ("limit=1001", "limit"),
        ("limit=hello", "limit"),
        ("offset=-1", "offset"),
        ("offset=9223372036854775808", "offset"),
        ("after_row_number=-1", "after_row_number"),
        ("after_row_number=9223372036854775808", "after_row_number"),
    ],
)
async def test_page_rejects_invalid_pagination_before_accessing_the_store(app, query, parameter):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"{ROWS}?{query}", headers=AUTH)

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["query", parameter]


@pytest.mark.parametrize("path", [ROWS, f"{ROWS}/stream"])
async def test_database_unavailable_is_reported_as_json_before_headers(app, settings, path):
    class UnavailablePool:
        def acquire(self, *, timeout):
            assert timeout == settings.acquire_timeout_seconds
            return self

        async def __aenter__(self):
            raise OSError("internal database endpoint")

        async def __aexit__(self, *_):
            pytest.fail("a connection should not have been acquired")

    app.state.store = Store(settings, UnavailablePool())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(path, headers=AUTH)

    assert response.status_code == 503
    assert response.headers["content-type"] == "application/json"
    assert response.json()["error"] == "database_unavailable"
    assert "internal database endpoint" not in response.text


async def test_initial_cursor_failure_returns_503_before_any_stream_headers(app):
    class FailedCursor:
        async def __anext__(self):
            raise WriterError(503, "database_unavailable", "Result read is unavailable")

    class FailedCursorStore:
        @asynccontextmanager
        async def stream_rows(self, upload_id):
            assert upload_id == UPLOAD_ID
            yield 1, FailedCursor()

    app.state.store = FailedCursorStore()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"{ROWS}/stream", headers=AUTH)

    assert response.status_code == 503
    assert "X-Row-Count" not in response.headers
    assert response.json()["error"] == "database_unavailable"


async def run_interrupted_stream(app, spec_version, *, disconnect=True, block=True):
    """ASGITransport buffers responses and cannot simulate a failed socket write."""
    path = f"{ROWS}/stream"
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": spec_version},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "root_path": "",
        "query_string": b"",
        "headers": [(b"authorization", AUTH["Authorization"].encode())],
    }
    sent = []
    first_body = asyncio.Event()

    async def receive():
        if not disconnect:
            await asyncio.Future()
        await first_body.wait()
        return {"type": "http.disconnect"}

    async def send(message):
        sent.append(message)
        if message["type"] == "http.response.body" and message.get("more_body"):
            if not block:
                return
            first_body.set()
            if disconnect and spec_version == "2.4":
                raise OSError("client disconnected during a send")
            await asyncio.Future()  # Stop making progress until the disconnect is read.

    async with asyncio.timeout(3):
        if disconnect:
            try:
                await app(scope, receive, send)
            except (OSError, ClientDisconnect):
                assert spec_version == "2.4"
        elif block:
            with pytest.raises(RuntimeError):
                await app(scope, receive, send)
        else:
            await app(scope, receive, send)

    assert sent[0]["type"] == "http.response.start"
    assert sent[0]["status"] == 200
    assert first_body.is_set() == block
    finished = any(m["type"] == "http.response.body" and not m.get("more_body") for m in sent)
    assert finished != block
    return [m["body"] for m in sent if m["type"] == "http.response.body" and m.get("more_body")]


class StreamStore:
    open = False

    @asynccontextmanager
    async def stream_rows(self, upload_id):
        assert upload_id == UPLOAD_ID
        self.open = True

        async def rows():
            yield '{"row_number":1,"data":{"value":"' + "x" * (128 * 1024) + '"}}'
            pytest.fail("the server should stop reading when the client stops receiving")

        try:
            yield 2, rows()
        finally:
            await asyncio.sleep(0)
            self.open = False


@pytest.mark.parametrize("spec_version,disconnect", [("2.3", True), ("2.4", True), ("2.3", False)])
async def test_interruption_closes_stream_context(app, settings, spec_version, disconnect):
    if not disconnect:
        settings.export_timeout_seconds = 0.05
    store = StreamStore()
    app.state.store = store
    await run_interrupted_stream(app, spec_version, disconnect=disconnect)

    assert not store.open


async def test_export_batches_small_rows_and_preserves_wide_utf8_rows(app):
    rows = [
        '{"row_number":1,"data":{"value":"é"}}',
        '{"row_number":2,"data":{"value":2}}',
        '{"row_number":3,"data":{"value":"' + "я" * (128 * 1024) + '"}}',
        '{"row_number":4,"data":{"value":4}}',
    ]

    class BatchStore:
        @asynccontextmanager
        async def stream_rows(self, _):
            async def records():
                for row in rows:
                    yield row

            yield len(rows), records()

    app.state.store = BatchStore()
    chunks = await run_interrupted_stream(app, "2.4", disconnect=False, block=False)
    assert chunks[0] == ("\n".join(rows[:2]) + "\n").encode()
    assert max(map(len, chunks)) == 128 * 1024
    assert b"".join(chunks) == ("\n".join(rows) + "\n").encode()


async def test_oversized_page_stops_reading_at_the_byte_budget(app, settings, monkeypatch):
    class Connection:
        @asynccontextmanager
        async def transaction(self, **kwargs):
            yield

        async def fetchval(self, *_):
            return 3

        async def cursor(self, *_, **kwargs):
            yield ('{"row_number":1,"data":{}}',)
            yield ('{"row_number":2,"data":{}}',)
            pytest.fail("the server should stop fetching as soon as the page exceeds the limit")

    class Pool:
        @asynccontextmanager
        async def acquire(self, **kwargs):
            yield Connection()

    monkeypatch.setattr("async_postgres_writer.writer.MAX_PAGE_BYTES", 40)
    app.state.store = Store(settings, Pool())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(ROWS, headers=AUTH)

    assert response.status_code == 413
    assert response.json()["error"] == "page_too_large"


@pytest.mark.parametrize("timeout", [0, float("inf")])
def test_export_timeout_must_be_positive_and_finite(timeout):
    with pytest.raises(ValidationError, match="export_timeout_seconds"):
        Settings(
            database_url="postgresql://localhost/test",
            api_token=TOKEN,
            export_timeout_seconds=timeout,
        )
