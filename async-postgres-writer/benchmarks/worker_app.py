"""Expose the serving worker PID on the isolated benchmark endpoint."""

import os

from async_postgres_writer.app import create_app

from .typed_writer import create_typed_app


def create_worker_app():
    app = {"jsonb": create_app, "columns": create_typed_app}[os.environ["BENCHMARK_LAYOUT"]]()
    worker_header = (b"x-benchmark-worker-pid", str(os.getpid()).encode("ascii"))

    async def identified_app(scope, receive, send):
        async def identified_send(message):
            if message["type"] == "http.response.start":
                message["headers"] = [*message.get("headers", []), worker_header]
            await send(message)

        await app(scope, receive, identified_send)

    return identified_app
