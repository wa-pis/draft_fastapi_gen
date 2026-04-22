from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import JSON, bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.settings import get_settings
from app.domain.events import DomainEvent


settings = get_settings()


@dataclass(kw_only=True, frozen=True, slots=True)
class OutboxRepository:
    session: AsyncSession

    async def add(self, event: DomainEvent) -> None:
        await add_outbox_event(self.session, event)

    async def claim_pending_batch(self, limit: int) -> list[dict]:
        return await fetch_pending_batch(self.session, limit)

    async def mark_published(self, event_id: int) -> None:
        await mark_published(self.session, event_id)

    async def mark_failed(self, event_id: int, error: str) -> None:
        await mark_failed(self.session, event_id, error)

    async def count_by_status(self) -> dict[str, int]:
        return await count_by_status(self.session)


async def add_outbox_event(session: AsyncSession, event: DomainEvent) -> None:
    await session.execute(
        text(
            """
            INSERT INTO outbox_events
                (aggregate_type, aggregate_id, event_type, payload, headers, occurred_at, status)
            VALUES
                (:aggregate_type, :aggregate_id, :event_type, :payload, :headers, :occurred_at, 'pending')
            """
        ).bindparams(
            bindparam("payload", type_=JSON),
            bindparam("headers", type_=JSON),
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
            WHERE
                status = 'pending'
                OR (
                    status = 'failed'
                    AND attempt_count < :max_attempts
                    AND next_attempt_at <= :now
                )
            ORDER BY id
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """
        ),
        {"limit": limit, "max_attempts": settings.OUTBOX_MAX_ATTEMPTS, "now": datetime.now(UTC)},
    )
    rows = result.mappings().all()
    ids = [row["id"] for row in rows]
    if ids:
        await session.execute(
            text(
                """
                UPDATE outbox_events
                SET
                    status = 'publishing',
                    attempt_count = attempt_count + 1,
                    next_attempt_at = :next_attempt_at
                WHERE id = ANY(:ids)
                """
            ),
            {
                "ids": ids,
                "next_attempt_at": datetime.now(UTC)
                + timedelta(seconds=settings.OUTBOX_RETRY_DELAY_SECONDS),
            },
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
        {"id": event_id, "now": datetime.now(UTC)},
    )


async def mark_failed(session: AsyncSession, event_id: int, error: str) -> None:
    next_attempt_at = datetime.now(UTC) + timedelta(seconds=settings.OUTBOX_RETRY_DELAY_SECONDS)
    await session.execute(
        text(
            """
            UPDATE outbox_events
            SET
                status = CASE
                    WHEN attempt_count >= :max_attempts THEN 'dead_letter'
                    ELSE 'failed'
                END,
                error = :error,
                next_attempt_at = :next_attempt_at
            WHERE id = :id
            """
        ),
        {
            "id": event_id,
            "error": error,
            "next_attempt_at": next_attempt_at,
            "max_attempts": settings.OUTBOX_MAX_ATTEMPTS,
        },
    )


async def count_by_status(session: AsyncSession) -> dict[str, int]:
    result = await session.execute(
        text(
            """
            SELECT status, count(*) AS count
            FROM outbox_events
            GROUP BY status
            """
        )
    )
    return {row["status"]: row["count"] for row in result.mappings().all()}
