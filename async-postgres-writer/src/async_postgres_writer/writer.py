import asyncio
import hashlib
from collections.abc import AsyncIterable, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import datetime
from uuid import UUID

import asyncpg

from .config import Settings
from .errors import WriterError
from .stream import NDJSONStream

_RECEIPT = "SELECT * FROM stream_writer.uploads WHERE upload_id = $1"
_EXPORT_RECORD = "jsonb_build_object('row_number', row_number, 'data', data)::text"
_EXPORT_ROWS = (
    f"SELECT {_EXPORT_RECORD} FROM stream_writer.rows WHERE upload_id = $1 ORDER BY row_number"
)
_EXPORT_ROWS_AFTER = (
    f"SELECT {_EXPORT_RECORD}, row_number FROM stream_writer.rows "
    "WHERE upload_id = $1 AND row_number > $2 ORDER BY row_number LIMIT $3"
)
_READ_ERRORS = (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError)
_EXPORT_PREFETCH = 8
MAX_PAGE_BYTES = 16 * 1024**2


@dataclass(frozen=True)
class Receipt:
    upload_id: UUID
    sha256: str
    row_count: int
    byte_count: int
    committed_at: datetime
    reused: bool = False


def _receipt(row: asyncpg.Record) -> Receipt:
    return Receipt(**dict(row))


class Store:
    supports_result_reads = True
    _rows_check = "SELECT upload_id, row_number, data FROM stream_writer.rows LIMIT 0"

    def __init__(self, settings: Settings, pool: asyncpg.Pool) -> None:
        self.settings = settings
        self.pool = pool

    async def check(self) -> None:
        try:
            async with self.pool.acquire(timeout=self.settings.acquire_timeout_seconds) as conn:
                await conn.fetchrow(
                    "SELECT upload_id, sha256, row_count, byte_count, committed_at "
                    "FROM stream_writer.uploads LIMIT 0"
                )
                await conn.fetchrow(self._rows_check)
        except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError) as exc:
            raise WriterError(503, "database_unavailable", "Database is not ready") from exc

    async def lookup(self, upload_id: UUID) -> Receipt | None:
        try:
            async with self.pool.acquire(timeout=self.settings.acquire_timeout_seconds) as conn:
                row = await conn.fetchrow(_RECEIPT, upload_id)
                return _receipt(row) if row is not None else None
        except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError) as exc:
            raise WriterError(503, "database_unavailable", "Receipt lookup is unavailable") from exc

    @asynccontextmanager
    async def _committed_read(
        self, upload_id: UUID
    ) -> AsyncIterator[tuple[asyncpg.Connection, int]]:
        try:
            async with (
                self.pool.acquire(timeout=self.settings.acquire_timeout_seconds) as conn,
                conn.transaction(isolation="repeatable_read", readonly=True),
            ):
                total = await conn.fetchval(
                    "SELECT row_count FROM stream_writer.uploads WHERE upload_id = $1", upload_id
                )
                if total is None:
                    raise WriterError(404, "upload_not_found", "Committed upload not found")
                yield conn, total
        except _READ_ERRORS as exc:
            raise WriterError(503, "database_unavailable", "Result read is unavailable") from exc

    async def page(
        self, upload_id: UUID, limit: int, offset: int = 0, *, after_row_number: int | None = None
    ) -> tuple[int, list[bytes], int | None]:
        async with self._committed_read(upload_id) as (conn, total):
            records: list[bytes] = []
            if after_row_number is None:
                if offset >= total:
                    return total, records, None
                query = _EXPORT_ROWS + " LIMIT $2 OFFSET $3"
                bounds = (limit, offset)
            else:
                query = _EXPORT_ROWS_AFTER
                bounds = (after_row_number, limit + 1)
            size = 0
            last_row_number = None
            async for row in conn.cursor(query, upload_id, *bounds, prefetch=_EXPORT_PREFETCH):
                if len(records) == limit:
                    return total, records, last_row_number
                record = row[0].encode("utf-8")
                size += len(record) + bool(records)  # JSON separators
                if size > MAX_PAGE_BYTES:
                    raise WriterError(
                        413, "page_too_large", "Use a smaller limit or stream the result"
                    )
                records.append(record)
                if after_row_number is not None:
                    last_row_number = row[1]
            return total, records, None

    @asynccontextmanager
    async def stream_rows(self, upload_id: UUID) -> AsyncIterator[tuple[int, AsyncIterator[str]]]:
        async with self._committed_read(upload_id) as (conn, total):
            cursor = conn.cursor(_EXPORT_ROWS, upload_id, prefetch=_EXPORT_PREFETCH)
            yield total, (row[0] async for row in cursor)

    async def write(self, upload_id: UUID, chunks: AsyncIterable[bytes]) -> Receipt:
        settings = self.settings
        stream = NDJSONStream(
            chunks,
            max_line_bytes=settings.max_line_bytes,
            max_upload_bytes=settings.max_upload_bytes,
            read_timeout_seconds=settings.read_timeout_seconds,
        )
        phase = "acquire"
        try:
            async with asyncio.timeout(settings.upload_timeout_seconds):
                async with self.pool.acquire(timeout=settings.acquire_timeout_seconds) as conn:
                    phase = "copy"
                    async with conn.transaction():
                        await conn.execute("SET LOCAL synchronous_commit = on")
                        key = int.from_bytes(
                            hashlib.sha256(upload_id.bytes).digest()[:8], signed=True
                        )
                        locked = await conn.fetchval("SELECT pg_try_advisory_xact_lock($1)", key)
                        if not locked:
                            raise WriterError(
                                409, "upload_in_progress", "This upload is in progress"
                            )
                        existing = await conn.fetchrow(_RECEIPT, upload_id)
                        if existing is not None:
                            async for _ in stream:
                                pass
                            receipt = _receipt(existing)
                            if (
                                receipt.sha256 != stream.sha256
                                or receipt.row_count != stream.row_count
                                or receipt.byte_count != stream.byte_count
                            ):
                                raise WriterError(
                                    409, "upload_conflict", "Upload ID has other data"
                                )
                            receipt = replace(receipt, reused=True)
                        else:
                            await conn.execute(
                                "INSERT INTO stream_writer.uploads "
                                "(upload_id, sha256, row_count, byte_count) VALUES ($1, $2, 0, 0)",
                                upload_id,
                                "0" * 64,
                            )

                            await self._copy_rows(conn, upload_id, stream)
                            row = await conn.fetchrow(
                                "UPDATE stream_writer.uploads SET sha256=$2, row_count=$3, "
                                "byte_count=$4, committed_at=clock_timestamp() "
                                "WHERE upload_id=$1 RETURNING *",
                                upload_id,
                                stream.sha256,
                                stream.row_count,
                                stream.byte_count,
                            )
                            receipt = _receipt(row)
                        phase = "commit"
                    phase = "release"
                return receipt
        except WriterError:
            raise
        except asyncpg.DataError as exc:
            if phase == "copy":
                raise WriterError(400, "invalid_stream", "A value is unsupported by JSONB") from exc
            raise self._unavailable(phase) from exc
        except TimeoutError as exc:
            if phase == "copy":
                raise WriterError(408, "upload_timeout", "Upload time limit exceeded") from exc
            raise self._unavailable(phase) from exc
        except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError) as exc:
            raise self._unavailable(phase) from exc

    async def _copy_rows(
        self, conn: asyncpg.Connection, upload_id: UUID, stream: NDJSONStream
    ) -> None:
        async def records() -> AsyncIterator[tuple[UUID, int, str]]:
            async for number, text in stream:
                yield upload_id, number, text

        await conn.copy_records_to_table(
            "rows",
            schema_name="stream_writer",
            columns=("upload_id", "row_number", "data"),
            records=records(),
            timeout=self.settings.upload_timeout_seconds,
        )

    @staticmethod
    def _unavailable(phase: str) -> WriterError:
        if phase in ("commit", "release"):
            return WriterError(
                503,
                "commit_unknown",
                "Commit response is unavailable; GET the receipt or retry the same upload ID",
            )
        return WriterError(
            503, "database_unavailable", "Database is unavailable; retry the same ID"
        )
