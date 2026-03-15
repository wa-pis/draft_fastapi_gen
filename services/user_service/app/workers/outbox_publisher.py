import asyncio
import json

from sqlalchemy.ext.asyncio import AsyncSession

from app.infra.db import get_db_session
from app.infra.kafka import KafkaProducer
from app.infra.outbox import fetch_pending_batch, mark_published, mark_failed
from app.observability.metrics import WORKER_THROUGHPUT


BATCH_SIZE = 100
TOPIC_HEADER = "topic"


async def publish_once(session: AsyncSession, producer: KafkaProducer) -> None:
    events = await fetch_pending_batch(session, BATCH_SIZE)
    for row in events:
        headers = row["headers"] or {}
        topic = headers.get(TOPIC_HEADER)
        if not topic:
            await mark_failed(session, row["id"], "missing topic header")
            continue
        try:
            payload_bytes = json.dumps(row["payload"]).encode("utf-8")
            await producer.send(topic, payload_bytes, headers=[("event_id", str(row["id"]).encode())])
            await mark_published(session, row["id"])
            WORKER_THROUGHPUT.labels(worker="outbox_publisher").inc()
        except Exception as exc:  # noqa: BLE001
            await mark_failed(session, row["id"], str(exc))
    await session.commit()


async def run() -> None:
    producer = KafkaProducer()
    await producer.start()
    try:
        while True:
            async with get_db_session() as session:
                await publish_once(session, producer)
            await asyncio.sleep(0.5)
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(run())

