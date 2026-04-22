import json
import secrets
from dataclasses import dataclass
from typing import Any, Protocol


class IdempotencyKeyConflictError(ValueError):
    pass


class RedisClient(Protocol):
    async def get(self, key: str) -> str | None: ...

    async def set(self, key: str, value: str, *, ex: int, nx: bool = False) -> bool: ...

    async def delete(self, key: str) -> None: ...

    async def eval(self, script: str, numkeys: int, *keys_and_args: str) -> Any: ...


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

    async def get_existing(self, key: str | None, request_hash: str) -> dict[str, Any] | None:
        if not key:
            return None
        raw = await self.redis.get(self.prefix + key)
        if not raw:
            return None
        stored = json.loads(raw)
        if stored.get("request_hash") != request_hash:
            raise IdempotencyKeyConflictError("idempotency key was used with a different request")
        response = stored.get("response")
        return response if isinstance(response, dict) else None

    async def acquire(self, key: str | None) -> str | None:
        if not key:
            return None
        token = secrets.token_urlsafe(32)
        acquired = bool(
            await self.redis.set(
                self.lock_prefix + key,
                token,
                ex=self.lock_ttl_seconds,
                nx=True,
            )
        )
        return token if acquired else None

    async def release(self, key: str | None, token: str | None) -> None:
        if not key or not token:
            return
        await self.redis.eval(
            """
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            end
            return 0
            """,
            1,
            self.lock_prefix + key,
            token,
        )

    async def store(self, key: str | None, request_hash: str, response: dict[str, Any]) -> None:
        if not key:
            return
        await self.redis.set(
            self.prefix + key,
            json.dumps({"request_hash": request_hash, "response": response}),
            ex=self.ttl_seconds,
        )
