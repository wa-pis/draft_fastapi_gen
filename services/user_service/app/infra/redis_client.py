from redis.asyncio import Redis

from app.config.settings import get_settings


settings = get_settings()
redis = Redis.from_url(str(settings.REDIS_URL), decode_responses=True)


async def redis_is_healthy() -> bool:
    try:
        await redis.ping()
        return True
    except Exception:
        return False

