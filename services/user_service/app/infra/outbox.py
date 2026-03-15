from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.events import DomainEvent


async def add_outbox_event(session: AsyncSession, event: DomainEvent) -> None:
    await session.execute(
        text(
            """
            INSERT INTO outbox_events
                (aggregate_type, aggregate_id, event_type, payload, headers, occurred_at, status)
            VALUES
                (:aggregate_type, :aggregate_id, :event_type, :payload, :headers, :occurred_at, 'pending')
            """
        ),
        {
            "aggregate_type": event.aggregate_type,
            "aggregate_id": event.aggregate_id,
            "event_type": event.event_type,
            "payload": event.payload,
            "headers": {"topic": "user.v1.events", "event_id": event.id},
            "occurred_at": event.occurred_at,
        },
    )


async def fetch_pending_batch(session: AsyncSession, limit: int = 100) -> list[dict]:
    result = await session.execute(
        text(
            """
            SELECT id, aggregate_type, aggregate_id, event_type, payload, headers
            FROM outbox_events
            WHERE status = 'pending'
            ORDER BY id
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """
        ),
        {"limit": limit},
    )
    rows = result.mappings().all()
    ids = [row["id"] for row in rows]
    if ids:
        await session.execute(
            text("UPDATE outbox_events SET status = 'publishing' WHERE id = ANY(:ids)"),
            {"ids": ids},
        )
    return [dict(row) for row in rows]


async def mark_published(session: AsyncSession, event_id: int) -> None:
    await session.execute(
        text(
            """
            UPDATE outbox_events
            SET status = 'published', published_at = :now
            WHERE id = :id
            """
        ),
        {"id": event_id, "now": datetime.utcnow()},
    )


async def mark_failed(session: AsyncSession, event_id: int, error: str) -> None:
    await session.execute(
        text(
            """
            UPDATE outbox_events
            SET status = 'failed', error = :error
            WHERE id = :id
            """
        ),
        {"id": event_id, "error": error},
    )

