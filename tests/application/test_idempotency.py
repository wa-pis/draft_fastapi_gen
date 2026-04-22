import pytest

from app.application.idempotency import IdempotencyKeyConflictError, IdempotencyService


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

    async def eval(self, script: str, numkeys: int, *keys_and_args: str) -> int:
        key, token = keys_and_args
        if self.values.get(key) == token:
            del self.values[key]
            return 1
        return 0


@pytest.mark.asyncio
async def test_store_and_get_existing_response() -> None:
    fake_redis = FakeRedis()
    service = IdempotencyService(ttl_seconds=60, redis_client=fake_redis)

    await service.store(
        "request-1",
        "hash-1",
        {"id": "user-1", "created_at": "2026-04-22T12:00:00Z"},
    )

    assert await service.get_existing("request-1", "hash-1") == {
        "id": "user-1",
        "created_at": "2026-04-22T12:00:00Z",
    }


@pytest.mark.asyncio
async def test_existing_response_rejects_payload_hash_mismatch() -> None:
    fake_redis = FakeRedis()
    service = IdempotencyService(ttl_seconds=60, redis_client=fake_redis)

    await service.store("request-1", "hash-1", {"id": "user-1"})

    with pytest.raises(IdempotencyKeyConflictError):
        await service.get_existing("request-1", "hash-2")


@pytest.mark.asyncio
async def test_acquire_is_atomic_for_same_key() -> None:
    fake_redis = FakeRedis()
    service = IdempotencyService(lock_ttl_seconds=30, redis_client=fake_redis)

    first_acquire = await service.acquire("request-1")
    second_acquire = await service.acquire("request-1")

    assert first_acquire is not None
    assert second_acquire is None


@pytest.mark.asyncio
async def test_release_only_deletes_owned_lock() -> None:
    fake_redis = FakeRedis()
    service = IdempotencyService(lock_ttl_seconds=30, redis_client=fake_redis)

    first_token = await service.acquire("request-1")
    assert first_token is not None
    fake_redis.values["idem:lock:request-1"] = "second-token"

    await service.release("request-1", first_token)

    assert fake_redis.values["idem:lock:request-1"] == "second-token"
