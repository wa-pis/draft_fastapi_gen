import hashlib
import json
import logging

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.schemas import UserCreateRequest, UserResponse
from app.application.idempotency import IdempotencyKeyConflictError, IdempotencyService
from app.application.user_service import UserService
from app.config.settings import get_settings
from app.infra.db import get_db_session
from app.infra.db import db_is_healthy
from app.infra.kafka import kafka_is_healthy
from app.infra.outbox import OutboxRepository
from app.infra.repositories import DuplicateUserEmailError, UserRepository
from app.infra.redis_client import redis_is_healthy


router = APIRouter()
settings = get_settings()
logger = logging.getLogger(__name__)


def request_fingerprint(payload: UserCreateRequest) -> str:
    canonical_payload = json.dumps(
        payload.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(f"POST:/v1/users:{canonical_payload}".encode("utf-8")).hexdigest()


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
    idem = IdempotencyService(
        ttl_seconds=settings.IDEMPOTENCY_TTL_SECONDS,
        lock_ttl_seconds=settings.IDEMPOTENCY_LOCK_TTL_SECONDS,
    )
    request_hash = request_fingerprint(payload)
    try:
        existing = await idem.get_existing(idempotency_key, request_hash)
    except IdempotencyKeyConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail="idempotency key was used with a different request",
        ) from exc
    if existing:
        return UserResponse(**existing)

    lock_token = await idem.acquire(idempotency_key)
    if idempotency_key and not lock_token:
        raise HTTPException(status_code=409, detail="request with this idempotency key is still processing")

    service = UserService(
        session=session,
        user_repository=UserRepository(session=session),
        outbox_repository=OutboxRepository(session=session),
    )
    try:
        user = await service.create_user(str(payload.email), payload.name)
        response = UserResponse(
            id=user.id,
            email=user.email,
            name=user.name,
            created_at=user.created_at,
        )
        try:
            await idem.store(idempotency_key, request_hash, response.model_dump(mode="json"))
        except Exception:
            logger.exception("failed to store idempotency response", extra={"idempotency_key": idempotency_key})
        return response
    except DuplicateUserEmailError as exc:
        await session.rollback()
        raise HTTPException(status_code=409, detail="email already exists") from exc
    except Exception:
        await session.rollback()
        raise
    finally:
        try:
            await idem.release(idempotency_key, lock_token)
        except Exception:
            logger.exception("failed to release idempotency lock", extra={"idempotency_key": idempotency_key})


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
