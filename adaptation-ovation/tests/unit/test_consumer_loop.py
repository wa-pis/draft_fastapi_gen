from __future__ import annotations

from collections import deque
from collections.abc import Sequence
from threading import Event

import pytest

from calculation_worker.application.consumer_loop import ConsumedRecord, ConsumerLoop
from calculation_worker.domain.models import HandlerResult, MessageContext, OutgoingRecord
from calculation_worker.errors import ConsumeError, PublishError
from calculation_worker.infrastructure.observability import Metrics
from calculation_worker.settings import Settings


class FakeConsumer:
    def __init__(
        self,
        records: Sequence[ConsumedRecord],
        stop_signal: Event,
        timeline: list[str],
        *,
        stop_during_first_poll: bool = False,
        fail_on_commit: bool = False,
    ) -> None:
        self._records = deque(records)
        self._stop_signal = stop_signal
        self._timeline = timeline
        self._stop_during_first_poll = stop_during_first_poll
        self._fail_on_commit = fail_on_commit
        self.subscribed_topic: str | None = None
        self.poll_count = 0
        self.commits: list[ConsumedRecord] = []

    def subscribe(self, topic: str) -> None:
        self.subscribed_topic = topic

    def poll(self, _timeout: float) -> ConsumedRecord | None:
        self.poll_count += 1
        if not self._records:
            self._stop_signal.set()
            return None
        record = self._records.popleft()
        if self._stop_during_first_poll and self.poll_count == 1:
            self._stop_signal.set()
        return record

    def commit(self, record: ConsumedRecord) -> None:
        self._timeline.append(f"commit:{record.offset}")
        if self._fail_on_commit:
            raise ConsumeError("simulated commit failure")
        self.commits.append(record)

    def close(self) -> None:
        pass


class FakeProcessor:
    def __init__(
        self,
        results: Sequence[HandlerResult],
        error: Exception | None = None,
    ) -> None:
        self._results = deque(results)
        self._error = error
        self.calls: list[tuple[bytes, MessageContext]] = []

    def process(self, payload: bytes, context: MessageContext) -> HandlerResult:
        self.calls.append((payload, context))
        if self._error is not None:
            raise self._error
        return self._results.popleft()


class FakePublisher:
    def __init__(
        self,
        timeline: list[str],
        *,
        fail_on_call: int | None = None,
        stop_on_publish: Event | None = None,
    ) -> None:
        self._timeline = timeline
        self._fail_on_call = fail_on_call
        self._stop_on_publish = stop_on_publish
        self.batches: list[tuple[OutgoingRecord, ...]] = []

    def publish_and_wait(self, records: Sequence[OutgoingRecord]) -> None:
        batch = tuple(records)
        self.batches.append(batch)
        self._timeline.append(f"publish:{len(batch)}")
        if self._stop_on_publish is not None:
            self._stop_on_publish.set()
        if self._fail_on_call == len(self.batches):
            raise PublishError("simulated producer failure")

    def close(self) -> None:
        pass


def test_publishes_all_records_before_single_commit() -> None:
    timeline: list[str] = []
    records = [_consumed(10)]
    outgoing = (_outgoing("completed"), _outgoing("audit"))
    loop, consumer, processor, publisher = _loop(
        records,
        [HandlerResult(records=outgoing, outcome="completed")],
        timeline,
    )

    loop.run()

    assert timeline == ["publish:2", "commit:10"]
    assert consumer.commits == records
    assert publisher.batches == [outgoing]
    assert processor.calls[0][1].raw_value == b"payload-10"


def test_publish_failure_does_not_commit_or_poll_next_message() -> None:
    timeline: list[str] = []
    loop, consumer, _processor, publisher = _loop(
        [_consumed(1), _consumed(2)],
        [
            HandlerResult(records=(_outgoing("first"),), outcome="completed"),
            HandlerResult(records=(_outgoing("second"),), outcome="completed"),
        ],
        timeline,
        fail_on_publish=1,
    )

    with pytest.raises(PublishError, match="simulated producer failure"):
        loop.run()

    assert consumer.poll_count == 1
    assert consumer.commits == []
    assert len(publisher.batches) == 1


def test_publish_failure_inside_handler_does_not_commit() -> None:
    timeline: list[str] = []
    loop, consumer, processor, publisher = _loop(
        [_consumed(1), _consumed(2)],
        [],
        timeline,
        processor_error=PublishError("started publication failed"),
    )

    with pytest.raises(PublishError, match="started publication failed"):
        loop.run()

    assert consumer.poll_count == 1
    assert len(processor.calls) == 1
    assert publisher.batches == []
    assert consumer.commits == []


def test_failed_event_is_published_before_commit() -> None:
    timeline: list[str] = []
    failed = _outgoing("failed")
    loop, consumer, _processor, publisher = _loop(
        [_consumed(7)],
        [HandlerResult(records=(failed,), outcome="failed")],
        timeline,
    )

    loop.run()

    assert publisher.batches == [(failed,)]
    assert timeline == ["publish:1", "commit:7"]
    assert len(consumer.commits) == 1


def test_ignored_event_commits_without_publishing() -> None:
    timeline: list[str] = []
    loop, consumer, _processor, publisher = _loop(
        [_consumed(3)],
        [HandlerResult(records=(), outcome="ignored")],
        timeline,
    )

    loop.run()

    assert publisher.batches == []
    assert timeline == ["commit:3"]
    assert len(consumer.commits) == 1


def test_three_messages_are_processed_and_committed_sequentially() -> None:
    timeline: list[str] = []
    loop, consumer, processor, publisher = _loop(
        [_consumed(1), _consumed(2), _consumed(3)],
        [
            HandlerResult(records=(_outgoing("one"),), outcome="completed"),
            HandlerResult(records=(_outgoing("two"),), outcome="completed"),
            HandlerResult(records=(_outgoing("three"),), outcome="completed"),
        ],
        timeline,
    )

    loop.run()

    assert timeline == [
        "publish:1",
        "commit:1",
        "publish:1",
        "commit:2",
        "publish:1",
        "commit:3",
    ]
    assert len(processor.calls) == len(publisher.batches) == len(consumer.commits) == 3


def test_business_failure_does_not_prevent_processing_next_message() -> None:
    timeline: list[str] = []
    failed_records = (_outgoing("failed"),)
    loop, consumer, _processor, publisher = _loop(
        [_consumed(1), _consumed(2)],
        [
            HandlerResult(records=failed_records, outcome="failed"),
            HandlerResult(records=(_outgoing("completed"),), outcome="completed"),
        ],
        timeline,
    )

    loop.run()

    assert publisher.batches == [failed_records, (_outgoing("completed"),)]
    assert [record.offset for record in consumer.commits] == [1, 2]


def test_stop_set_during_poll_does_not_start_or_commit_returned_message() -> None:
    timeline: list[str] = []
    loop, consumer, processor, publisher = _loop(
        [_consumed(99)],
        [HandlerResult(records=(_outgoing("unused"),), outcome="completed")],
        timeline,
        stop_during_first_poll=True,
    )

    loop.run()

    assert consumer.poll_count == 1
    assert processor.calls == []
    assert publisher.batches == []
    assert consumer.commits == []


def test_commit_failure_stops_before_polling_next_message() -> None:
    timeline: list[str] = []
    loop, consumer, _processor, _publisher = _loop(
        [_consumed(1), _consumed(2)],
        [
            HandlerResult(records=(_outgoing("one"),), outcome="completed"),
            HandlerResult(records=(_outgoing("two"),), outcome="completed"),
        ],
        timeline,
        fail_on_commit=True,
    )

    with pytest.raises(ConsumeError, match="simulated commit failure"):
        loop.run()

    assert consumer.poll_count == 1
    assert consumer.commits == []
    assert timeline == ["publish:1", "commit:1"]


def test_stop_during_processing_finishes_publish_and_commit() -> None:
    timeline: list[str] = []
    loop, consumer, _processor, publisher = _loop(
        [_consumed(4), _consumed(5)],
        [HandlerResult(records=(_outgoing("four"),), outcome="completed")],
        timeline,
        stop_during_publish=True,
    )

    loop.run()

    assert publisher.batches == [(_outgoing("four"),)]
    assert timeline == ["publish:1", "commit:4"]
    assert consumer.poll_count == 1
    assert [record.offset for record in consumer.commits] == [4]


def _loop(
    records: Sequence[ConsumedRecord],
    results: Sequence[HandlerResult],
    timeline: list[str],
    *,
    fail_on_publish: int | None = None,
    stop_during_first_poll: bool = False,
    fail_on_commit: bool = False,
    stop_during_publish: bool = False,
    processor_error: Exception | None = None,
) -> tuple[ConsumerLoop, FakeConsumer, FakeProcessor, FakePublisher]:
    stop_signal = Event()
    consumer = FakeConsumer(
        records,
        stop_signal,
        timeline,
        stop_during_first_poll=stop_during_first_poll,
        fail_on_commit=fail_on_commit,
    )
    processor = FakeProcessor(results, processor_error)
    publisher = FakePublisher(
        timeline,
        fail_on_call=fail_on_publish,
        stop_on_publish=stop_signal if stop_during_publish else None,
    )
    loop = ConsumerLoop(
        consumer=consumer,
        processor=processor,  # type: ignore[arg-type]
        publisher=publisher,
        stop_signal=stop_signal,
        settings=_settings(),
        metrics=Metrics(),
    )
    return loop, consumer, processor, publisher


def _consumed(offset: int) -> ConsumedRecord:
    return ConsumedRecord(
        topic="INTEGRATIONS",
        partition=0,
        offset=offset,
        key=f"key-{offset}".encode(),
        value=f"payload-{offset}".encode(),
        headers=(("trace", b"value"),),
        native_message=object(),
    )


def _outgoing(name: str, *, topic: str = "INTEGRATIONS") -> OutgoingRecord:
    return OutgoingRecord(
        topic=topic,
        key=f"request-{name}",
        value={"name": name},
    )


def _settings() -> Settings:
    return Settings.model_validate(
        {
            "UPSTREAM_API_BASE_URL": "http://upstream.test",
            "UPSTREAM_API_TOKEN": "secret",
            "DBOS_SYSTEM_DATABASE_URL": "postgresql://test:test@localhost/test",
        }
    )
