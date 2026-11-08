import traceback
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import ValidationError
from starlette.requests import ClientDisconnect

from async_postgres_writer import __main__ as cli
from async_postgres_writer.app import create_app
from async_postgres_writer.config import Settings
from async_postgres_writer.writer import Receipt, WriterError

TOKEN = "a" * 32
AUTH = {"Authorization": f"Bearer {TOKEN}"}
NDJSON = {**AUTH, "Content-Type": "application/x-ndjson"}


class FakeStore:
    def __init__(self):
        self.receipt = None
        self.received = None
        self.ready = False
        self.error = None

    async def write(self, upload_id, chunks):
        if self.error:
            raise self.error
        self.received = b"".join([chunk async for chunk in chunks])
        self.receipt = Receipt(
            upload_id=upload_id,
            sha256="f" * 64,
            row_count=1,
            byte_count=len(self.received),
            committed_at=datetime(2026, 11, 8, tzinfo=UTC),
        )
        return self.receipt

    async def lookup(self, upload_id):
        if self.error:
            raise self.error
        return self.receipt

    async def check(self):
        if self.error:
            raise self.error
        self.ready = True


@pytest.fixture
def app_store():
    settings = Settings(
        database_url="postgresql://localhost/test",
        api_token=TOKEN,
        max_upload_bytes=100,
    )
    app = create_app(settings)
    store = FakeStore()
    app.state.store = store
    return app, store


@pytest.mark.asyncio
async def test_upload_streams_and_serializes_receipt(app_store):
    app, store = app_store
    upload_id = uuid4()

    async def chunks():
        yield b'{"value":'
        yield b"1}\n"

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        response = await c.put(f"/v1/uploads/{upload_id}", headers=NDJSON, content=chunks())
        assert response.status_code == 201
        assert response.json()["upload_id"] == str(upload_id)
        assert response.json()["committed_at"] == "2026-11-08T00:00:00+00:00"
        assert store.received == b'{"value":1}\n'
        lookup = await c.get(f"/v1/uploads/{upload_id}", headers=AUTH)
        assert lookup.json() == response.json()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers,status",
    [
        ({}, 401),
        ({"Authorization": "Bearer wrong"}, 401),
        ({**AUTH, "Content-Type": "application/json"}, 415),
        ({**NDJSON, "Content-Type": "application/x-ndjson; charset=latin-1"}, 415),
        ({**NDJSON, "Content-Encoding": "gzip"}, 415),
        ({**NDJSON, "Content-Length": "101"}, 413),
        ({**NDJSON, "Content-Length": "-1"}, 400),
        ({**NDJSON, "Content-Length": "not-an-int"}, 400),
    ],
)
async def test_upload_rejects_headers_before_reading(app_store, headers, status):
    app, store = app_store
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        response = await c.put(f"/v1/uploads/{uuid4()}", headers=headers, content=b"{}\n")
    assert response.status_code == status
    assert store.received is None


@pytest.mark.asyncio
async def test_health_auth_and_missing_upload(app_store):
    app, store = app_store
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        assert (await c.get("/health/live")).status_code == 200
        assert (await c.get("/health/ready")).status_code == 401
        assert (await c.get(f"/v1/uploads/{uuid4()}")).status_code == 401
        assert (await c.get("/health/ready", headers=AUTH)).status_code == 200
        assert store.ready
        assert (await c.get(f"/v1/uploads/{uuid4()}", headers=AUTH)).status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error,status,code",
    [
        (WriterError(503, "commit_unknown", "Use GET to confirm upload"), 503, "commit_unknown"),
        (ClientDisconnect(), 400, "client_disconnected"),
    ],
)
async def test_safe_upload_errors_include_upload_id(app_store, error, status, code):
    app, store = app_store
    store.error = error
    upload_id = uuid4()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        response = await c.put(f"/v1/uploads/{upload_id}", headers=NDJSON, content=b"{}\n")
    assert response.status_code == status
    assert response.json()["error"] == code
    assert response.json()["upload_id"] == str(upload_id)


@pytest.mark.asyncio
async def test_upload_id_is_uuid(app_store):
    app, store = app_store
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        response = await c.put("/v1/uploads/invalid", headers=NDJSON, content=b"{}\n")
    assert response.status_code == 422
    assert store.received is None


@pytest.mark.parametrize(
    "overrides",
    [
        {"api_token": "short"},
        {"api_token": "a" * 31 + " "},
        {"api_token": "é" * 32},
        {"database_url": "https://not-postgresql"},
        {"pool_size": 0},
        {"pool_size": 33},
        {"max_upload_bytes": 0},
        {"max_line_bytes": 0},
        {"read_timeout_seconds": 0},
        {"upload_timeout_seconds": -1},
        {"acquire_timeout_seconds": 0},
        {"db_command_timeout_seconds": 0},
        {"read_timeout_seconds": float("inf")},
        {"upload_timeout_seconds": float("inf")},
        {"acquire_timeout_seconds": float("inf")},
        {"db_command_timeout_seconds": float("nan")},
    ],
)
def test_settings_reject_invalid_config(overrides):
    values = {"api_token": TOKEN, "database_url": "postgresql://localhost/test", **overrides}
    with pytest.raises(ValidationError):
        Settings(**values)


def test_settings_reads_env_and_hides_secrets(monkeypatch):
    monkeypatch.setenv("WRITER_DATABASE_URL", "postgresql://test:secret@localhost/db")
    monkeypatch.setenv("WRITER_API_TOKEN", TOKEN)
    monkeypatch.setenv("WRITER_POOL_SIZE", "2")
    settings = Settings()
    assert settings.pool_size == 2
    assert "secret" not in repr(settings)
    assert TOKEN not in repr(settings)
    with pytest.raises(ValidationError) as error:
        Settings(database_url="https://secret-value")
    assert "secret-value" not in str(error.value)


@pytest.mark.asyncio
async def test_reused_upload_returns_200(app_store):
    app, store = app_store

    async def write(upload_id, chunks):
        return Receipt(
            upload_id=upload_id,
            sha256="f" * 64,
            row_count=1,
            byte_count=3,
            committed_at=datetime(2026, 11, 8, tzinfo=UTC),
            reused=True,
        )

    store.write = write
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        response = await c.put(f"/v1/uploads/{UUID(int=1)}", headers=NDJSON, content=b"{}\n")
    assert response.status_code == 200
    assert response.json()["reused"] is True


def test_cli_configuration_error_never_prints_credentials(monkeypatch, capsys):
    monkeypatch.setenv("WRITER_DATABASE_URL", "https://private-password@localhost/db")
    monkeypatch.setenv("WRITER_API_TOKEN", TOKEN)
    monkeypatch.setattr("sys.argv", ["async-postgres-writer", "init-db"])
    assert cli.main() == 2
    output = capsys.readouterr()
    assert "private-password" not in output.err
    assert "configuration" in output.err


def test_cli_database_error_never_prints_credentials(monkeypatch, capsys):
    monkeypatch.setenv("WRITER_DATABASE_URL", "postgresql://localhost/db")
    monkeypatch.setenv("WRITER_API_TOKEN", TOKEN)
    monkeypatch.setattr("sys.argv", ["async-postgres-writer", "init-db"])

    async def fail(settings):
        raise OSError("private-password connection rejected")

    monkeypatch.setattr(cli, "init_db", fail)
    assert cli.main() == 1
    output = capsys.readouterr()
    assert "private-password" not in output.err
    assert "initialization failed" in output.err


@pytest.mark.asyncio
async def test_startup_database_error_suppresses_original_credentials(app_store, monkeypatch):
    app, _ = app_store

    async def fail(*args, **kwargs):
        raise OSError("private-password connection rejected")

    monkeypatch.setattr("async_postgres_writer.app.asyncpg.create_pool", fail)
    with pytest.raises(RuntimeError, match="Database connection unavailable") as error:
        async with app.router.lifespan_context(app):
            pytest.fail("Lifespan started without a database")
    assert "private-password" not in "".join(traceback.format_exception(error.value))
