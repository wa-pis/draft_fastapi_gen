"""Transactional batch and streaming I/O with Psycopg 3 (sync and asyncio)."""

import asyncio
from contextlib import closing
from pathlib import Path

import psycopg
from common import COLUMNS, TABLE, atomic_output, batches, parse_args, rows, write_row

COLUMN_LIST = ", ".join(COLUMNS)
INSERT = f"INSERT INTO {TABLE} ({COLUMN_LIST}) VALUES (%s, %s, %s)"
SELECT = f"SELECT {COLUMN_LIST} FROM {TABLE} ORDER BY id"
PAGE = f"{SELECT} LIMIT %s OFFSET %s"
COPY = f"COPY {TABLE} ({COLUMN_LIST}) FROM STDIN"
SNAPSHOT = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
DURABLE_WRITE = "SET LOCAL synchronous_commit = on"


def batch_write(dsn: str, path: Path, batch_size: int) -> int:
    count = 0
    with closing(rows(path)) as source, psycopg.connect(dsn) as conn, conn.cursor() as cursor:
        cursor.execute(DURABLE_WRITE)
        for batch in batches(source, batch_size):
            cursor.executemany(INSERT, batch)
            count += len(batch)
    return count


def batch_read(dsn: str, path: Path, batch_size: int) -> int:
    count = 0
    with atomic_output(path) as output, psycopg.connect(dsn) as conn, conn.cursor() as cursor:
        cursor.execute(SNAPSHOT)
        while page := cursor.execute(PAGE, (batch_size, count)).fetchall():
            for row in page:
                write_row(output, row)
            count += len(page)
            if len(page) < batch_size:
                break
    return count


def stream_write(dsn: str, path: Path) -> int:
    count = 0
    with closing(rows(path)) as source, psycopg.connect(dsn) as conn, conn.cursor() as cursor:
        cursor.execute(DURABLE_WRITE)
        with cursor.copy(COPY) as copy:
            for row in source:
                copy.write_row(row)
                count += 1
    return count


def stream_read(dsn: str, path: Path, batch_size: int) -> int:
    count = 0
    with atomic_output(path) as output, psycopg.connect(dsn) as conn:
        conn.execute(SNAPSHOT)
        with conn.cursor(name="postgres_io_stream") as cursor:
            cursor.itersize = batch_size
            cursor.execute(SELECT)
            for row in cursor:
                write_row(output, row)
                count += 1
    return count


async def async_batch_write(dsn: str, path: Path, batch_size: int) -> int:
    count = 0
    with closing(rows(path)) as source:
        conn = await psycopg.AsyncConnection.connect(dsn)
        async with conn, conn.cursor() as cursor:
            await cursor.execute(DURABLE_WRITE)
            for batch in batches(source, batch_size):
                await cursor.executemany(INSERT, batch)
                count += len(batch)
    return count


async def async_batch_read(dsn: str, path: Path, batch_size: int) -> int:
    count = 0
    with atomic_output(path) as output:
        conn = await psycopg.AsyncConnection.connect(dsn)
        async with conn, conn.cursor() as cursor:
            await cursor.execute(SNAPSHOT)
            while True:
                await cursor.execute(PAGE, (batch_size, count))
                page = await cursor.fetchall()
                for row in page:
                    write_row(output, row)
                count += len(page)
                if len(page) < batch_size:
                    break
    return count


async def async_stream_write(dsn: str, path: Path) -> int:
    count = 0
    with closing(rows(path)) as source:
        conn = await psycopg.AsyncConnection.connect(dsn)
        async with conn, conn.cursor() as cursor:
            await cursor.execute(DURABLE_WRITE)
            async with cursor.copy(COPY) as copy:
                for row in source:
                    await copy.write_row(row)
                    count += 1
    return count


async def async_stream_read(dsn: str, path: Path, batch_size: int) -> int:
    count = 0
    with atomic_output(path) as output:
        conn = await psycopg.AsyncConnection.connect(dsn)
        async with conn:
            await conn.execute(SNAPSHOT)
            async with conn.cursor(name="postgres_io_stream") as cursor:
                cursor.itersize = batch_size
                await cursor.execute(SELECT)
                async for row in cursor:
                    write_row(output, row)
                    count += 1
    return count


def main() -> None:
    args = parse_args(with_async_flag=True)
    sync, async_ = {
        "batch-write": (batch_write, async_batch_write),
        "batch-read": (batch_read, async_batch_read),
        "stream-write": (stream_write, async_stream_write),
        "stream-read": (stream_read, async_stream_read),
    }[args.action]
    params = (args.dsn, args.path)
    cli_params = params if args.action == "stream-write" else (*params, args.batch_size)
    count = asyncio.run(async_(*cli_params)) if args.use_async else sync(*cli_params)
    print(f"{args.action}: {count} rows")


if __name__ == "__main__":
    main()
