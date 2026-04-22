import asyncio
import json
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from app.config.settings import get_settings
from app.infra.outbox import count_by_status, fetch_pending_batch, mark_failed, mark_published

if TYPE_CHECKING:
    from app.infra.kafka import KafkaProducer


TOPIC_HEADER = "topic"
settings = get_settings()


@asynccontextmanager
async def get_db_session() -> AsyncIterator[Any]:
    from app.infra.db import get_db_session as open_db_session

    async with open_db_session() as session:
        yield session


def observe_worker_message(worker: str, status: str, duration: float) -> None:
    from app.observability.metrics import observe_worker_message as observe

    observe(worker, status, duration)


def set_outbox_backlog(status: str, count: int) -> None:
    from app.observability.metrics import set_outbox_backlog as set_backlog

    set_backlog(status, count)


def _build_kafka_headers(headers: dict[str, Any]) -> list[tuple[str, bytes]]:
    return [
        (key, str(value).encode("utf-8"))
        for key, value in headers.items()
        if key != TOPIC_HEADER and value is not None
    ]


async def _publish_event(
    row: dict[str, Any],
    producer: "KafkaProducer",
    semaphore: asyncio.Semaphore,
) -> tuple[int, str | None]:
    started = time.perf_counter()
    async with semaphore:
        headers = row["headers"] or {}
        topic = headers.get(TOPIC_HEADER)
        if not topic:
            observe_worker_message("outbox_publisher", "failed", time.perf_counter() - started)
            return row["id"], "missing topic header"
        try:
            payload_bytes = json.dumps(row["payload"], separators=(",", ":")).encode("utf-8")
            await producer.send(topic, payload_bytes, headers=_build_kafka_headers(headers))
            observe_worker_message("outbox_publisher", "published", time.perf_counter() - started)
            return row["id"], None
        except Exception as exc:  # noqa: BLE001
            observe_worker_message("outbox_publisher", "failed", time.perf_counter() - started)
            return row["id"], str(exc)


async def publish_once(producer: "KafkaProducer") -> int:
    async with get_db_session() as session:
        events = await fetch_pending_batch(session, settings.OUTBOX_BATCH_SIZE)
        for status, count in (await count_by_status(session)).items():
            set_outbox_backlog(status, count)
        await session.commit()

    if not events:
        return 0

    semaphore = asyncio.Semaphore(settings.OUTBOX_PUBLISH_CONCURRENCY)
    results = await asyncio.gather(
        *(_publish_event(row, producer, semaphore) for row in events),
    )

    async with get_db_session() as session:
        for event_id, error in results:
            if error is None:
                await mark_published(session, event_id)
            else:
                await mark_failed(session, event_id, error)
        await session.commit()

    return len(events)


async def run() -> None:
    from app.infra.kafka import KafkaProducer

    producer = KafkaProducer()
    await producer.start()
    try:
        while True:
            processed = await publish_once(producer)
            if not processed:
                await asyncio.sleep(settings.OUTBOX_IDLE_SLEEP_SECONDS)
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(run())
