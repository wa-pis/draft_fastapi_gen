import argparse
import asyncio
import sys
from importlib.resources import files

import asyncpg
import uvicorn
from pydantic import ValidationError

from .app import create_app
from .config import Settings


async def init_db(settings: Settings) -> None:
    connection = await asyncpg.connect(
        settings.database_url.get_secret_value(),
        timeout=settings.acquire_timeout_seconds,
        command_timeout=settings.db_command_timeout_seconds,
    )
    try:
        async with connection.transaction():
            await connection.execute(
                files("async_postgres_writer").joinpath("schema.sql").read_text()
            )
    finally:
        await connection.close(timeout=10)


def main() -> int:
    parser = argparse.ArgumentParser(description="Stream NDJSON into PostgreSQL atomically")
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="Run the HTTP service")
    serve.add_argument("--host", default="0.0.0.0")
    serve.add_argument("--port", default=8080, type=int)
    commands.add_parser(
        "init-db", help="Create the service tables; requires schema-owner privileges"
    )
    args = parser.parse_args()
    try:
        settings = Settings()
    except ValidationError:
        print("Invalid WRITER_ configuration; check required values and limits", file=sys.stderr)
        return 2
    if args.command == "serve":
        uvicorn.run(create_app(settings), host=args.host, port=args.port)
        return 0
    try:
        asyncio.run(init_db(settings))
    except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError, ValueError):
        print(
            "Database initialization failed; check connection and schema privileges",
            file=sys.stderr,
        )
        return 1
    print("Database schema initialized")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
