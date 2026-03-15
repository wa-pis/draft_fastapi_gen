import time
import uuid

from fastapi import FastAPI, Request

from app.api.routes import router as api_router
from app.config.settings import get_settings
from app.observability.logging import setup_logging
from app.observability.metrics import router as metrics_router
from app.observability.tracing import setup_tracing


settings = get_settings()
setup_logging(settings)
setup_tracing(settings)

app = FastAPI(title="User Service", version="1.0.0")


@app.middleware("http")
async def correlation_middleware(request: Request, call_next):
    correlation_id = request.headers.get("x-correlation-id") or str(uuid.uuid4())
    request_id = str(uuid.uuid4())

    start = time.perf_counter()
    response = await call_next(request)
    duration = time.perf_counter() - start

    response.headers["x-correlation-id"] = correlation_id
    response.headers["x-request-id"] = request_id
    # metrics are observed in route handlers in this skeleton
    return response


app.include_router(api_router)
app.include_router(metrics_router)

