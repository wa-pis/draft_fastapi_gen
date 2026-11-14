import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict
from email.message import Message
from secrets import compare_digest
from typing import Annotated
from uuid import UUID

import asyncpg
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel
from starlette.requests import ClientDisconnect
from starlette.types import Message as ASGIMessage
from starlette.types import Receive, Scope, Send

from .config import Settings
from .writer import MAX_PAGE_BYTES, Store, WriterError

_NO_STORE = {"Cache-Control": "no-store"}
_NDJSON = "application/x-ndjson"


class _ResultRow(BaseModel):
    row_number: int
    data: dict[str, object]


class _ResultPage(BaseModel):
    upload_id: UUID
    row_count: int
    limit: int
    offset: int
    rows: list[_ResultRow]


class _ResultStream(StreamingResponse):
    media_type = _NDJSON

    def __init__(self, upload_id: UUID, store: Store, timeout: float) -> None:
        super().__init__((), headers=_NO_STORE)
        self.upload_id = upload_id
        self.store = store
        self.timeout = timeout

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        started = False

        async def track_start(message: ASGIMessage) -> None:
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
            await send(message)

        # This task owns the transaction even if Starlette sends in a separate task.
        try:
            async with (
                asyncio.timeout(self.timeout),
                self.store.stream_rows(self.upload_id) as (count, rows),
            ):
                first = await anext(rows, None)

                async def lines() -> AsyncIterator[str]:
                    sent = 0
                    if first is not None:
                        sent += 1
                        yield first + "\n"
                    async for row in rows:
                        sent += 1
                        yield row + "\n"
                    if sent != count:
                        raise RuntimeError("Result export did not match its receipt")

                self.headers["X-Row-Count"] = str(count)
                self.body_iterator = lines()
                await super().__call__(scope, receive, track_start)
        except ClientDisconnect:
            return
        except (WriterError, TimeoutError) as exc:
            if started:
                raise RuntimeError("Result stream interrupted") from None
            if isinstance(exc, WriterError):
                raise
            raise WriterError(503, "export_timeout", "Result export timed out") from None


def create_app(settings: Settings | None = None, *, store_type: type[Store] = Store) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        try:
            pool = await asyncpg.create_pool(
                settings.database_url.get_secret_value(),
                min_size=1,
                max_size=settings.pool_size,
                timeout=settings.acquire_timeout_seconds,
                command_timeout=settings.db_command_timeout_seconds,
            )
        except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError, ValueError):
            raise RuntimeError("Database connection unavailable") from None
        app.state.store = store_type(settings, pool)
        try:
            yield
        finally:
            try:
                async with asyncio.timeout(10):
                    await pool.close()
            except TimeoutError:
                pool.terminate()

    app = FastAPI(title="Async PostgreSQL Stream Writer", lifespan=lifespan)

    async def authenticate(authorization: Annotated[str | None, Header()] = None) -> None:
        scheme, _, token = (authorization or "").partition(" ")
        expected = settings.api_token.get_secret_value().encode("ascii")
        if scheme.lower() != "bearer" or not compare_digest(token.encode("utf-8"), expected):
            raise HTTPException(
                401, "Valid Bearer token required", headers={"WWW-Authenticate": "Bearer"}
            )

    @app.exception_handler(WriterError)
    async def writer_error_handler(request: Request, error: WriterError) -> JSONResponse:
        content = {"error": error.code, "message": error.message}
        if "upload_id" in request.path_params:
            content["upload_id"] = str(request.path_params["upload_id"])
        return JSONResponse(
            status_code=error.status_code,
            content=content,
        )

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready", dependencies=[Depends(authenticate)])
    async def ready(request: Request) -> dict[str, str]:
        await request.app.state.store.check()
        return {"status": "ok"}

    @app.put("/v1/uploads/{upload_id}", dependencies=[Depends(authenticate)])
    async def upload(upload_id: UUID, request: Request) -> JSONResponse:
        content_type = Message()
        content_type["content-type"] = request.headers.get("content-type", "")
        if content_type.get_content_type() != "application/x-ndjson" or (
            content_type.get_content_charset("utf-8").lower() != "utf-8"
        ):
            raise HTTPException(415, "Content-Type must be application/x-ndjson; charset=utf-8")
        if request.headers.get("content-encoding", "identity").lower() != "identity":
            raise HTTPException(415, "Content-Encoding is not supported")
        length = request.headers.get("content-length")
        if length is not None:
            try:
                size = int(length)
            except ValueError:
                raise HTTPException(400, "Invalid Content-Length") from None
            if size < 0:
                raise HTTPException(400, "Invalid Content-Length")
            if size > settings.max_upload_bytes:
                raise HTTPException(413, "Upload exceeds configured byte limit")
        try:
            receipt = await request.app.state.store.write(upload_id, request.stream())
        except ClientDisconnect:
            raise WriterError(400, "client_disconnected", "Upload interrupted by client") from None
        return JSONResponse(
            status_code=200 if receipt.reused else 201,
            content=jsonable_encoder(asdict(receipt)),
        )

    @app.get("/v1/uploads/{upload_id}", dependencies=[Depends(authenticate)])
    async def lookup(upload_id: UUID, request: Request) -> JSONResponse:
        receipt = await request.app.state.store.lookup(upload_id)
        if receipt is None:
            raise HTTPException(404, "Committed upload not found")
        return JSONResponse(content=jsonable_encoder(asdict(receipt)))

    if store_type.supports_result_reads:

        @app.get(
            "/v1/uploads/{upload_id}/rows",
            dependencies=[Depends(authenticate)],
            response_model=_ResultPage,
        )
        async def result_page(
            upload_id: UUID,
            request: Request,
            limit: Annotated[int, Query(ge=1, le=1000)] = 100,
            offset: Annotated[int, Query(ge=0, le=2**63 - 1)] = 0,
        ) -> Response:
            total, rows = await request.app.state.store.page(upload_id, limit, offset)
            metadata = json.dumps(
                {"upload_id": str(upload_id), "row_count": total, "limit": limit, "offset": offset},
                separators=(",", ":"),
            ).encode()
            prefix = metadata[:-1] + b',"rows":['
            if len(prefix) + sum(map(len, rows)) + max(0, len(rows) - 1) + 2 > MAX_PAGE_BYTES:
                raise WriterError(413, "page_too_large", "Use a smaller limit or stream the result")
            # Send the database's JSON without decoding arbitrary-precision numbers.
            body = b"".join((prefix, b",".join(rows), b"]}"))
            return Response(body, media_type="application/json", headers=_NO_STORE)

        @app.get(
            "/v1/uploads/{upload_id}/rows/stream",
            dependencies=[Depends(authenticate)],
            responses={
                200: {
                    "content": {_NDJSON: {}},
                    "headers": {"X-Row-Count": {"schema": {"type": "integer"}}},
                }
            },
            response_class=Response,
        )
        async def result_stream(upload_id: UUID, request: Request) -> Response:
            return _ResultStream(
                upload_id, request.app.state.store, settings.export_timeout_seconds
            )

    return app
