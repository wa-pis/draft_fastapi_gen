import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict
from email.message import Message
from secrets import compare_digest
from typing import Annotated
from uuid import UUID

import asyncpg
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from starlette.requests import ClientDisconnect

from .config import Settings
from .writer import Store, WriterError


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

    return app
