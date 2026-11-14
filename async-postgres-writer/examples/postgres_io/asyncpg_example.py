"""Native asyncio examples using asyncpg."""

import asyncio
from contextlib import closing
from pathlib import Path

import asyncpg
from common import COLUMNS, TABLE, atomic_output, batches, parse_args, rows, write_row

COLUMN_LIST = ", ".join(COLUMNS)
INSERT = f"INSERT INTO {TABLE} ({COLUMN_LIST}) VALUES ($1, $2, $3)"
SELECT = f"SELECT {COLUMN_LIST} FROM {TABLE} ORDER BY id"
PAGE = f"{SELECT} LIMIT $1 OFFSET $2"
DURABLE_WRITE = "SET LOCAL synchronous_commit = on"


async def batch_write(conn: asyncpg.Connection, path: Path, size: int) -> int:
    count = 0
    with closing(rows(path)) as source:
        async with conn.transaction():
            await conn.execute(DURABLE_WRITE)
            for batch in batches(source, size):
                await conn.executemany(INSERT, batch)
                count += len(batch)
    return count


async def batch_read(conn: asyncpg.Connection, path: Path, size: int) -> int:
    offset = 0
    with atomic_output(path) as output:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            while page := await conn.fetch(PAGE, size, offset):
                for row in page:
                    write_row(output, row)
                offset += len(page)
                if len(page) < size:
                    break
    return offset


async def stream_write(conn: asyncpg.Connection, path: Path) -> int:
    schema, table = TABLE.split(".")
    with closing(rows(path)) as source:
        async with conn.transaction():
            await conn.execute(DURABLE_WRITE)
            status = await conn.copy_records_to_table(
                table, schema_name=schema, records=source, columns=COLUMNS
            )
    return int(status.split()[-1])


async def stream_read(conn: asyncpg.Connection, path: Path, size: int) -> int:
    count = 0
    with atomic_output(path) as output:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            async for row in conn.cursor(SELECT, prefetch=size):
                write_row(output, row)
                count += 1
    return count


async def main() -> None:
    args = parse_args()
    conn = await asyncpg.connect(args.dsn)
    try:
        match args.action:
            case "batch-write":
                count = await batch_write(conn, args.path, args.batch_size)
            case "batch-read":
                count = await batch_read(conn, args.path, args.batch_size)
            case "stream-write":
                count = await stream_write(conn, args.path)
            case "stream-read":
                count = await stream_read(conn, args.path, args.batch_size)
    finally:
        await conn.close()
    print(f"{args.action}: {count} rows")


if __name__ == "__main__":
    asyncio.run(main())
