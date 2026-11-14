"""SQLAlchemy Core with the asyncpg PostgreSQL dialect."""

import asyncio
from contextlib import closing
from pathlib import Path

from common import COLUMNS, TABLE, atomic_output, batches, parse_args, rows, write_row
from sqlalchemy import BigInteger, Column, Integer, MetaData, Table, Text, make_url, select
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

schema, table_name = TABLE.split(".")
table = Table(
    table_name,
    MetaData(),
    Column("id", BigInteger, primary_key=True),
    Column("name", Text, nullable=False),
    Column("value", Integer, nullable=False),
    schema=schema,
)
ordered_rows = select(table).order_by(table.c.id)


async def batch_write(engine: AsyncEngine, path: Path, batch_size: int) -> int:
    count = 0
    with closing(rows(path)) as source:
        async with engine.begin() as connection:
            await connection.exec_driver_sql("SET LOCAL synchronous_commit = on")
            for batch in batches(source, batch_size):
                await connection.execute(
                    table.insert(), [dict(zip(COLUMNS, row, strict=True)) for row in batch]
                )
                count += len(batch)
    return count


async def stream_write(engine: AsyncEngine, path: Path) -> int:
    with closing(rows(path)) as source:
        async with engine.begin() as connection:
            # Start the actual driver transaction through SQLAlchemy before raw COPY.
            await connection.exec_driver_sql("SET LOCAL synchronous_commit = on")
            raw = await connection.get_raw_connection()
            status = await raw.driver_connection.copy_records_to_table(
                table_name, schema_name=schema, columns=COLUMNS, records=source
            )
    return int(status.split()[-1])


async def batch_read(engine: AsyncEngine, path: Path, batch_size: int) -> int:
    offset = 0
    with atomic_output(path) as output:
        async with engine.begin() as connection:
            await connection.exec_driver_sql(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
            )
            while True:
                page = (
                    await connection.execute(ordered_rows.limit(batch_size).offset(offset))
                ).all()
                for row in page:
                    write_row(output, row)
                offset += len(page)
                if len(page) < batch_size:
                    break
    return offset


async def stream_read(engine: AsyncEngine, path: Path, batch_size: int) -> int:
    count = 0
    with atomic_output(path) as output:
        async with engine.begin() as connection:
            await connection.exec_driver_sql(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
            )
            async with connection.stream(
                ordered_rows, execution_options={"yield_per": batch_size}
            ) as result:
                async for batch in result.partitions(batch_size):
                    for row in batch:
                        write_row(output, row)
                    count += len(batch)
    return count


async def main() -> None:
    args = parse_args()
    url = make_url(args.dsn).set(drivername="postgresql+asyncpg")
    engine = create_async_engine(url)
    try:
        if args.action == "stream-write":
            count = await stream_write(engine, args.path)
        else:
            operation = {
                "batch-write": batch_write,
                "batch-read": batch_read,
                "stream-read": stream_read,
            }[args.action]
            count = await operation(engine, args.path, args.batch_size)
    finally:
        await engine.dispose()
    print(f"{args.action}: {count} rows")


if __name__ == "__main__":
    asyncio.run(main())
