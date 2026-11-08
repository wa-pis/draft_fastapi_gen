import asyncio
import hashlib
from collections.abc import AsyncIterable
from dataclasses import dataclass, replace
from datetime import datetime
from uuid import UUID

import asyncpg

from .config import Settings
from .errors import WriterError
from .stream import NDJSONStream

_RECEIPT = "SELECT * FROM stream_writer.uploads WHERE upload_id = $1"


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
                await conn.fetchrow(
                    "SELECT upload_id, row_number, data FROM stream_writer.rows LIMIT 0"
                )
        except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError) as exc:
            raise WriterError(503, "database_unavailable", "Database is not ready") from exc

    async def lookup(self, upload_id: UUID) -> Receipt | None:
        try:
            async with self.pool.acquire(timeout=self.settings.acquire_timeout_seconds) as conn:
                row = await conn.fetchrow(_RECEIPT, upload_id)
                return _receipt(row) if row is not None else None
        except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError) as exc:
            raise WriterError(503, "database_unavailable", "Receipt lookup is unavailable") from exc

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

                            async def records():
                                async for number, text in stream:
                                    yield upload_id, number, text

                            await conn.copy_records_to_table(
                                "rows",
                                schema_name="stream_writer",
                                columns=("upload_id", "row_number", "data"),
                                records=records(),
                                timeout=settings.upload_timeout_seconds,
                            )
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
