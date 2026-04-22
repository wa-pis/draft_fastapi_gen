import time
import uuid

from fastapi import FastAPI, Request
from starlette.responses import Response

from app.api.routes import router as api_router
from app.config.settings import get_settings
from app.observability.logging import setup_logging
from app.observability.metrics import observe_request, router as metrics_router
from app.observability.tracing import instrument_fastapi, setup_tracing


settings = get_settings()
setup_logging(settings)
setup_tracing(settings)

app = FastAPI(title="User Service", version="1.0.0")
instrument_fastapi(app)


@app.middleware("http")
async def correlation_middleware(request: Request, call_next):
    correlation_id = request.headers.get("x-correlation-id") or str(uuid.uuid4())
    request_id = str(uuid.uuid4())

    start = time.perf_counter()
    response: Response | None = None
    try:
        response = await call_next(request)
        return response
    finally:
        duration = time.perf_counter() - start
        status_code = response.status_code if response is not None else 500
        observe_request(request.method, request.url.path, status_code, duration)

        if response is not None:
            response.headers["x-correlation-id"] = correlation_id
            response.headers["x-request-id"] = request_id


app.include_router(api_router)
app.include_router(metrics_router)
