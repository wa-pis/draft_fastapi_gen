import json
from dataclasses import dataclass
from typing import Any, Protocol


class RedisClient(Protocol):
    async def get(self, key: str) -> str | None: ...

    async def set(self, key: str, value: str, *, ex: int, nx: bool = False) -> bool: ...

    async def delete(self, key: str) -> None: ...


@dataclass(kw_only=True, frozen=True, slots=True)
class IdempotencyService:
    prefix: str = "idem:api:"
    lock_prefix: str = "idem:lock:"
    ttl_seconds: int = 3600
    lock_ttl_seconds: int = 30
    redis_client: RedisClient | None = None

    @property
    def redis(self) -> RedisClient:
        if self.redis_client is not None:
            return self.redis_client
        from app.infra.redis_client import redis

        return redis

    async def get_existing(self, key: str | None) -> dict[str, Any] | None:
        if not key:
            return None
        raw = await self.redis.get(self.prefix + key)
        return json.loads(raw) if raw else None

    async def acquire(self, key: str | None) -> bool:
        if not key:
            return True
        return bool(
            await self.redis.set(
                self.lock_prefix + key,
                "1",
                ex=self.lock_ttl_seconds,
                nx=True,
            )
        )

    async def release(self, key: str | None) -> None:
        if key:
            await self.redis.delete(self.lock_prefix + key)

    async def store(self, key: str | None, response: dict[str, Any]) -> None:
        if not key:
            return
        await self.redis.set(self.prefix + key, json.dumps(response), ex=self.ttl_seconds)
