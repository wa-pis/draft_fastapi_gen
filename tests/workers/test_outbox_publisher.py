from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest

from app.workers import outbox_publisher


class FakeProducer:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.messages: list[tuple[str, bytes, list[tuple[str, bytes]] | None]] = []

    async def send(
        self,
        topic: str,
        value: bytes,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        if self.fail:
            raise RuntimeError("kafka unavailable")
        self.messages.append((topic, value, headers))


class FakeSession:
    def __init__(self) -> None:
        self.commits = 0

    async def commit(self) -> None:
        self.commits += 1


@pytest.mark.asyncio
async def test_publish_event_sends_compact_json_and_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    producer = FakeProducer()
    row = {
        "id": 42,
        "payload": {"user_id": "u-1", "email": "user@example.com"},
        "headers": {"topic": "user.v1.events", "event_id": "evt-1"},
    }

    observed: list[tuple[str, str]] = []
    monkeypatch.setattr(
        outbox_publisher,
        "observe_worker_message",
        lambda worker, status, duration: observed.append((worker, status)),
    )

    event_id, error = await outbox_publisher._publish_event(
        row,
        producer,  # type: ignore[arg-type]
        outbox_publisher.asyncio.Semaphore(1),
    )

    assert event_id == 42
    assert error is None
    assert producer.messages == [
        (
            "user.v1.events",
            b'{"user_id":"u-1","email":"user@example.com"}',
            [("event_id", b"evt-1")],
        )
    ]
    assert observed == [("outbox_publisher", "published")]


@pytest.mark.asyncio
async def test_publish_event_requires_topic_header(monkeypatch: pytest.MonkeyPatch) -> None:
    producer = FakeProducer()
    row = {"id": 42, "payload": {"user_id": "u-1"}, "headers": {"event_id": "evt-1"}}
    observed: list[tuple[str, str]] = []
    monkeypatch.setattr(
        outbox_publisher,
        "observe_worker_message",
        lambda worker, status, duration: observed.append((worker, status)),
    )

    event_id, error = await outbox_publisher._publish_event(
        row,
        producer,  # type: ignore[arg-type]
        outbox_publisher.asyncio.Semaphore(1),
    )

    assert event_id == 42
    assert error == "missing topic header"
    assert producer.messages == []
    assert observed == [("outbox_publisher", "failed")]


@pytest.mark.asyncio
async def test_publish_once_claims_commits_and_marks_results(monkeypatch: pytest.MonkeyPatch) -> None:
    sessions = [FakeSession(), FakeSession()]
    events = [
        {
            "id": 1,
            "payload": {"user_id": "u-1"},
            "headers": {"topic": "user.v1.events", "event_id": "evt-1"},
        },
        {"id": 2, "payload": {"user_id": "u-2"}, "headers": {}},
    ]
    published: list[int] = []
    failed: list[tuple[int, str]] = []
    backlog_samples: list[tuple[str, int]] = []

    @asynccontextmanager
    async def fake_get_db_session() -> AsyncIterator[FakeSession]:
        yield sessions.pop(0)

    async def fake_fetch_pending_batch(session: FakeSession, limit: int) -> list[dict]:
        assert limit == outbox_publisher.settings.OUTBOX_BATCH_SIZE
        return events

    async def fake_count_by_status(session: FakeSession) -> dict[str, int]:
        return {"pending": 2}

    async def fake_mark_published(session: FakeSession, event_id: int) -> None:
        published.append(event_id)

    async def fake_mark_failed(session: FakeSession, event_id: int, error: str) -> None:
        failed.append((event_id, error))

    monkeypatch.setattr(outbox_publisher, "get_db_session", fake_get_db_session)
    monkeypatch.setattr(outbox_publisher, "fetch_pending_batch", fake_fetch_pending_batch)
    monkeypatch.setattr(outbox_publisher, "count_by_status", fake_count_by_status)
    monkeypatch.setattr(outbox_publisher, "mark_published", fake_mark_published)
    monkeypatch.setattr(outbox_publisher, "mark_failed", fake_mark_failed)
    monkeypatch.setattr(
        outbox_publisher,
        "set_outbox_backlog",
        lambda status, count: backlog_samples.append((status, count)),
    )
    monkeypatch.setattr(outbox_publisher, "observe_worker_message", lambda worker, status, duration: None)

    processed = await outbox_publisher.publish_once(FakeProducer())  # type: ignore[arg-type]

    assert processed == 2
    assert published == [1]
    assert failed == [(2, "missing topic header")]
    assert backlog_samples == [("pending", 2)]
