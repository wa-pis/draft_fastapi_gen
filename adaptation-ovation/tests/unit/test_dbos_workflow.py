from __future__ import annotations

import inspect
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

import pytest
from dbos import DBOS, EnqueueOptions
from prometheus_client import CollectorRegistry

from calculation_worker.application.calculation_workflow import register_calculation_workflow
from calculation_worker.application.event_factory import EventFactory
from calculation_worker.calculations.registry import CalculationRegistry
from calculation_worker.domain.models import (
    CalculationCompleted,
    CalculationFailed,
    CalculationJob,
    CalculationRequested,
    CalculationResult,
    OutgoingRecord,
)
from calculation_worker.errors import TerminalCalculationError, UpstreamTimeoutError
from calculation_worker.infrastructure.dbos_queue import (
    CALCULATION_QUEUE_NAME,
    CALCULATION_WORKFLOW_CLASS,
    CALCULATION_WORKFLOW_INSTANCE,
    CALCULATION_WORKFLOW_NAME,
    DBOSCalculationQueue,
    calculation_workflow_id,
)
from calculation_worker.infrastructure.observability import Metrics
from calculation_worker.settings import Settings


class NoopPublisher:
    def publish_and_wait(self, _records: Sequence[OutgoingRecord]) -> None:
        pass


class RecordingClient:
    def __init__(self) -> None:
        self.calls: list[tuple[EnqueueOptions, tuple[Any, ...]]] = []
        self.destroyed = False

    def enqueue(self, options: EnqueueOptions, *args: Any, **_kwargs: Any) -> None:
        self.calls.append((options, args))

    def destroy(self) -> None:
        self.destroyed = True


class RecordingPublisher:
    def __init__(self) -> None:
        self.batches: list[tuple[OutgoingRecord, ...]] = []

    def publish_and_wait(self, records: Sequence[OutgoingRecord]) -> None:
        self.batches.append(tuple(records))


class StaticCalculation:
    process_name = "example"

    def __init__(self, *, fail_fetch: bool = False) -> None:
        self.fail_fetch = fail_fetch
        self.fetch_calls = 0
        self.calculate_calls = 0

    def fetch_input(self, _request: CalculationRequested) -> dict[str, Any]:
        self.fetch_calls += 1
        if self.fail_fetch:
            raise UpstreamTimeoutError
        return {"one": 1, "two": 2}

    def calculate(
        self,
        _request: CalculationRequested,
        input_data: dict[str, Any],
    ) -> CalculationResult:
        self.calculate_calls += 1
        return CalculationResult(data={"count": len(input_data)})


@pytest.fixture(autouse=True)
def clean_dbos_registry() -> Any:
    yield
    DBOS.destroy(destroy_registry=True)


def test_every_workflow_step_disables_normal_retries() -> None:
    workflow = register_calculation_workflow(
        calculation_registry=CalculationRegistry(),
        event_factory=EventFactory("INTEGRATIONS", "worker"),
        metrics=Metrics(CollectorRegistry()),
        publisher=NoopPublisher(),
        worker_concurrency=4,
        max_recovery_attempts=3,
    )

    for name in (
        "publish_started",
        "fetch_input",
        "calculate",
        "publish_completed",
        "publish_failed",
    ):
        closure = inspect.getclosurevars(getattr(type(workflow), name)).nonlocals
        assert closure["retries_allowed"] is False

    workflow_info = type(workflow).run.dbos_func_decorator_info
    assert workflow_info.max_recovery_attempts == 3


def test_ingress_enqueues_json_payload_with_stable_dbos_identity() -> None:
    client = RecordingClient()
    queue = DBOSCalculationQueue(_settings(), cast(Any, client))
    job = _job()

    first_id = queue.enqueue(job)
    second_id = queue.enqueue(job)

    assert first_id == second_id == calculation_workflow_id("request-1", "example")
    options, args = client.calls[0]
    assert options == {
        "workflow_name": CALCULATION_WORKFLOW_NAME,
        "class_name": CALCULATION_WORKFLOW_CLASS,
        "instance_name": CALCULATION_WORKFLOW_INSTANCE,
        "queue_name": CALCULATION_QUEUE_NAME,
        "workflow_id": first_id,
        "app_version": "test-v1",
        "max_recovery_attempts": 3,
    }
    assert args == (job.model_dump(mode="json"),)
    queue.close()
    assert client.destroyed


def test_dbos_executes_the_sequential_workflow_and_checkpoints_steps(tmp_path: Path) -> None:
    calculation = StaticCalculation()
    publisher = RecordingPublisher()
    workflow = _launched_workflow(tmp_path, calculation, publisher)

    result = DBOS.start_workflow(
        cast(Any, workflow.run), _job().model_dump(mode="json")
    ).get_result()

    assert result == {"data": {"count": 2}}
    assert calculation.fetch_calls == calculation.calculate_calls == 1
    assert len(publisher.batches) == 2
    assert publisher.batches[0][0].value.event_type == "calculation.started"
    assert isinstance(publisher.batches[1][0].value, CalculationCompleted)


def test_terminal_step_error_is_not_retried_and_workflow_ends_in_error(tmp_path: Path) -> None:
    calculation = StaticCalculation(fail_fetch=True)
    publisher = RecordingPublisher()
    workflow = _launched_workflow(tmp_path, calculation, publisher)

    handle = DBOS.start_workflow(cast(Any, workflow.run), _job().model_dump(mode="json"))
    with pytest.raises(TerminalCalculationError, match="UPSTREAM_TIMEOUT"):
        handle.get_result()

    assert calculation.fetch_calls == 1
    assert calculation.calculate_calls == 0
    assert len(publisher.batches) == 2
    failed_batch = publisher.batches[1]
    assert len(failed_batch) == 1
    assert isinstance(failed_batch[0].value, CalculationFailed)
    assert failed_batch[0].value.error.retryable is False


def _settings() -> Settings:
    return Settings(
        _env_file=None,
        DBOS_SYSTEM_DATABASE_URL="postgresql://test:test@localhost/test",
        DBOS_APPLICATION_VERSION="test-v1",
        UPSTREAM_API_BASE_URL="http://unused.test",
        UPSTREAM_API_TOKEN="unused",
    )


def _job() -> CalculationJob:
    return CalculationJob.create(
        CalculationRequested(
            event_type="calculation.requested",
            schema_version=1,
            request_id="request-1",
            calc_id="calc-1",
            calc_process="example",
        )
    )


def _launched_workflow(
    tmp_path: Path,
    calculation: StaticCalculation,
    publisher: RecordingPublisher,
) -> Any:
    registry = CalculationRegistry()
    registry.register(calculation)
    workflow = register_calculation_workflow(
        calculation_registry=registry,
        event_factory=EventFactory("INTEGRATIONS", "worker"),
        metrics=Metrics(CollectorRegistry()),
        publisher=publisher,
        worker_concurrency=4,
        max_recovery_attempts=3,
    )
    DBOS(
        config={
            "name": "workflow-test",
            "system_database_url": f"sqlite:///{tmp_path / 'dbos.sqlite'}",
            "application_version": "test-v1",
            "executor_id": "test-executor",
            "run_migrations": True,
            "enable_otlp": False,
        }
    )
    DBOS.launch()
    return workflow
