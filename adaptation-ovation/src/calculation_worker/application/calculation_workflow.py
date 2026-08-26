from __future__ import annotations

import logging
from collections.abc import Mapping
from time import monotonic
from typing import Any

from dbos import DBOS, DBOSConfiguredInstance, Queue

from calculation_worker.application.contracts import PublisherPort
from calculation_worker.application.event_factory import (
    EventFactory,
    Failure,
    failure_for_exception,
)
from calculation_worker.calculations.registry import CalculationRegistry
from calculation_worker.domain.models import CalculationJob, CalculationRequested, CalculationResult
from calculation_worker.errors import PublishError, TerminalCalculationError
from calculation_worker.infrastructure.dbos_queue import (
    CALCULATION_QUEUE_NAME,
    CALCULATION_WORKFLOW_CLASS,
    CALCULATION_WORKFLOW_INSTANCE,
    CALCULATION_WORKFLOW_NAME,
)
from calculation_worker.infrastructure.observability import Metrics

logger = logging.getLogger(__name__)


def register_calculation_workflow(
    *,
    calculation_registry: CalculationRegistry,
    event_factory: EventFactory,
    metrics: Metrics,
    publisher: PublisherPort,
    worker_concurrency: int,
    max_recovery_attempts: int,
) -> DBOSConfiguredInstance:
    """Register the configured workflow instance before DBOS is launched."""

    Queue(CALCULATION_QUEUE_NAME, worker_concurrency=worker_concurrency)

    @DBOS.dbos_class(CALCULATION_WORKFLOW_CLASS)
    class CalculationWorkflow(DBOSConfiguredInstance):
        def __init__(self) -> None:
            self._calculation_registry = calculation_registry
            self._event_factory = event_factory
            self._metrics = metrics
            self._publisher = publisher
            super().__init__(CALCULATION_WORKFLOW_INSTANCE)

        @DBOS.workflow(
            name=CALCULATION_WORKFLOW_NAME,
            max_recovery_attempts=max_recovery_attempts,
        )
        def run(self, raw_job: dict[str, Any]) -> dict[str, Any]:
            job = CalculationJob.model_validate(raw_job)
            request_data = job.request.model_dump(mode="json")
            try:
                self._calculation_registry.get(job.request.calc_process)
                self.publish_started(request_data)
                input_data = self.fetch_input(request_data)
                result_data = self.calculate(request_data, input_data)
                self.publish_completed(request_data, result_data)
                return result_data
            except PublishError:
                raise
            except Exception as error:
                failure = failure_for_exception(error)
                self.publish_failed(raw_job, failure.code, failure.message)
                raise TerminalCalculationError(failure.code) from None

        @DBOS.step(name="calculation.publish_started", retries_allowed=False)
        def publish_started(self, request_data: dict[str, Any]) -> None:
            request = CalculationRequested.model_validate(request_data)
            self._publisher.publish_and_wait((self._event_factory.started(request),))
            logger.info(
                "Calculation started", extra={**_request_log_fields(request), "outcome": "started"}
            )

        @DBOS.step(name="calculation.fetch_input", retries_allowed=False)
        def fetch_input(self, request_data: dict[str, Any]) -> dict[str, Any]:
            request = CalculationRequested.model_validate(request_data)
            calculation = self._calculation_registry.get(request.calc_process)
            return dict(calculation.fetch_input(request))

        @DBOS.step(name="calculation.calculate", retries_allowed=False)
        def calculate(
            self,
            request_data: dict[str, Any],
            input_data: Mapping[str, Any],
        ) -> dict[str, Any]:
            request = CalculationRequested.model_validate(request_data)
            calculation = self._calculation_registry.get(request.calc_process)
            started_at = monotonic()
            result = calculation.calculate(request, input_data)
            duration = monotonic() - started_at
            self._metrics.calculations.labels(calculation.process_name, "completed").inc()
            self._metrics.calculation_duration.labels(calculation.process_name).observe(duration)
            logger.info(
                "Calculation completed",
                extra={
                    **_request_log_fields(request),
                    "outcome": "completed",
                    "processing_duration_ms": round(duration * 1000, 3),
                },
            )
            return result.model_dump(mode="json")

        @DBOS.step(name="calculation.publish_completed", retries_allowed=False)
        def publish_completed(
            self,
            request_data: dict[str, Any],
            result_data: dict[str, Any],
        ) -> None:
            request = CalculationRequested.model_validate(request_data)
            result = CalculationResult.model_validate(result_data)
            self._publisher.publish_and_wait((self._event_factory.completed(request, result),))

        @DBOS.step(name="calculation.publish_failed", retries_allowed=False)
        def publish_failed(self, raw_job: dict[str, Any], code: str, message: str) -> None:
            job = CalculationJob.model_validate(raw_job)
            failure = Failure(code, message)
            self._publisher.publish_and_wait(
                (
                    self._event_factory.failed(
                        job.request.request_id,
                        job.request.calc_id,
                        job.request.calc_process,
                        failure,
                    ),
                )
            )
            self._metrics.calculations.labels(job.request.calc_process, "failed").inc()
            logger.warning(
                "Calculation failed",
                extra={
                    **_request_log_fields(job.request),
                    "error_code": code,
                    "outcome": "failed",
                },
            )

    return CalculationWorkflow()


def _request_log_fields(request: CalculationRequested) -> dict[str, object]:
    return {
        "event_type": request.event_type,
        "request_id": request.request_id,
        "calc_id": request.calc_id,
        "calc_process": request.calc_process,
        "handler": CALCULATION_WORKFLOW_CLASS,
    }
