from __future__ import annotations

import asyncio
import time
from typing import Any

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from app.config.settings import get_settings
from app.resilience.circuit_breaker import CircuitBreaker
from app.resilience.retry import retry_with_backoff


settings = get_settings()
_health_cache: tuple[float, bool] = (0.0, False)


class KafkaProducer:
    def __init__(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            client_id=settings.KAFKA_CLIENT_ID,
        )
        self._cb = CircuitBreaker()

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def send(self, topic: str, value: bytes, headers: list[tuple[str, bytes]] | None = None) -> None:
        async def _send() -> Any:
            return await self._producer.send_and_wait(topic, value=value, headers=headers)

        await self._cb.call(lambda: retry_with_backoff(_send))


def build_consumer(
    topic: str,
    group_id: str,
) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        enable_auto_commit=False,
    )


async def kafka_is_healthy() -> bool:
    global _health_cache

    checked_at, is_healthy = _health_cache
    now = time.monotonic()
    if now - checked_at < settings.KAFKA_HEALTH_TTL_SECONDS:
        return is_healthy

    producer = KafkaProducer()
    try:
        await asyncio.wait_for(producer.start(), timeout=settings.KAFKA_HEALTH_TIMEOUT_SECONDS)
        _health_cache = (now, True)
        return True
    except Exception:
        _health_cache = (now, False)
        return False
    finally:
        try:
            await producer.stop()
        except Exception:
            pass
