import time

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.schemas import UserCreateRequest, UserResponse
from app.application.idempotency import IdempotencyService
from app.application.user_service import UserService
from app.infra.db import get_db_session
from app.infra.db import db_is_healthy
from app.infra.kafka import kafka_is_healthy
from app.infra.redis_client import redis_is_healthy
from app.observability.metrics import observe_request


router = APIRouter()


async def get_session() -> AsyncSession:
    async with get_db_session() as session:
        yield session


@router.post(
    "/v1/users",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_user(
    payload: UserCreateRequest,
    idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
    session: AsyncSession = Depends(get_session),
) -> UserResponse:
    idem = IdempotencyService()
    existing = await idem.get_existing(idempotency_key)
    if existing:
        return UserResponse(**existing)

    service = UserService(session)
    started = time.perf_counter()
    user = await service.create_user(payload.email, payload.name)
    duration = time.perf_counter() - started
    observe_request("POST", "/v1/users", 201, duration)

    response = UserResponse(
        id=user.id,
        email=user.email,
        name=user.name,
        created_at=user.created_at,
    )
    await idem.store(idempotency_key, response.model_dump())
    return response


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
async def ready() -> dict[str, str]:
    if not await db_is_healthy():
        raise HTTPException(status_code=503, detail="db unavailable")
    if not await kafka_is_healthy():
        raise HTTPException(status_code=503, detail="kafka unavailable")
    if not await redis_is_healthy():
        raise HTTPException(status_code=503, detail="redis unavailable")
    return {"status": "ready"}

