import pytest

from app.application.idempotency import IdempotencyService


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def set(self, key: str, value: str, *, ex: int, nx: bool = False) -> bool:
        if nx and key in self.values:
            return False
        self.values[key] = value
        return True

    async def delete(self, key: str) -> None:
        self.values.pop(key, None)


@pytest.mark.asyncio
async def test_store_and_get_existing_response() -> None:
    fake_redis = FakeRedis()
    service = IdempotencyService(ttl_seconds=60, redis_client=fake_redis)

    await service.store("request-1", {"id": "user-1", "created_at": "2026-04-22T12:00:00Z"})

    assert await service.get_existing("request-1") == {
        "id": "user-1",
        "created_at": "2026-04-22T12:00:00Z",
    }


@pytest.mark.asyncio
async def test_acquire_is_atomic_for_same_key() -> None:
    fake_redis = FakeRedis()
    service = IdempotencyService(lock_ttl_seconds=30, redis_client=fake_redis)

    first_acquire = await service.acquire("request-1")
    second_acquire = await service.acquire("request-1")

    assert first_acquire is True
    assert second_acquire is False
