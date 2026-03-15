from __future__ import annotations

from typing import Any

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from app.config.settings import get_settings
from app.resilience.circuit_breaker import CircuitBreaker
from app.resilience.retry import retry_with_backoff


settings = get_settings()


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
    # lightweight health check by attempting to start/stop a producer
    producer = KafkaProducer()
    try:
        await producer.start()
        return True
    except Exception:
        return False
    finally:
        try:
            await producer.stop()
        except Exception:
            pass

