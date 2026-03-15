import json

from app.infra.redis_client import redis


class IdempotencyService:
    def __init__(self, prefix: str = "idem:api:", ttl_seconds: int = 3600) -> None:
        self._prefix = prefix
        self._ttl = ttl_seconds

    async def get_existing(self, key: str | None) -> dict | None:
        if not key:
            return None
        raw = await redis.get(self._prefix + key)
        return json.loads(raw) if raw else None

    async def store(self, key: str | None, response: dict) -> None:
        if not key:
            return
        await redis.set(self._prefix + key, json.dumps(response), ex=self._ttl)

