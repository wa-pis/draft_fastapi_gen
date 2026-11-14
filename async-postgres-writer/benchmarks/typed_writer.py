"""Benchmark HTTP variant; uses the same receipt, replay and transaction as JSONB."""

from collections.abc import AsyncIterator
from uuid import UUID

import asyncpg
from fastapi import FastAPI

from async_postgres_writer.app import create_app
from async_postgres_writer.config import Settings
from async_postgres_writer.errors import WriterError
from async_postgres_writer.stream import NDJSONStream
from async_postgres_writer.writer import Store

from .typed_data import TYPED_COLUMNS, typed_record


class TypedStore(Store):
    _rows_check = f"SELECT {', '.join(TYPED_COLUMNS)} FROM stream_writer_bench.typed_rows LIMIT 0"

    async def _copy_rows(
        self, conn: asyncpg.Connection, upload_id: UUID, stream: NDJSONStream
    ) -> None:
        async def records() -> AsyncIterator[tuple]:
            async for number, row in stream.decoded_records():
                try:
                    yield typed_record(upload_id, number, row)
                except ValueError as exc:
                    raise WriterError(400, "invalid_stream", "Invalid typed benchmark row") from exc

        try:
            await conn.copy_records_to_table(
                "typed_rows",
                schema_name="stream_writer_bench",
                columns=TYPED_COLUMNS,
                records=records(),
                timeout=self.settings.upload_timeout_seconds,
            )
        except (
            asyncpg.DataError,
            asyncpg.CheckViolationError,
            asyncpg.NotNullViolationError,
        ) as exc:
            raise WriterError(400, "invalid_stream", "Invalid typed benchmark row") from exc


def create_typed_app(settings: Settings | None = None) -> FastAPI:
    return create_app(settings, store_type=TypedStore)
