from __future__ import annotations

from uuid import UUID, uuid5

from dbos import DBOSClient, EnqueueOptions

from calculation_worker.domain.models import CalculationJob
from calculation_worker.settings import Settings

CALCULATION_QUEUE_NAME = "calculation-queue"
CALCULATION_WORKFLOW_NAME = "calculation_workflow"
CALCULATION_WORKFLOW_CLASS = "CalculationWorkflow"
CALCULATION_WORKFLOW_INSTANCE = "default"
WORKFLOW_ID_NAMESPACE = UUID("bc22de8b-53f5-51db-89ec-14a6df757c35")


class DBOSCalculationQueue:
    """Ingress-side DBOS client; it persists work but executes no workflow code."""

    def __init__(self, settings: Settings, client: DBOSClient | None = None) -> None:
        self._application_version = settings.dbos_application_version
        self._max_recovery_attempts = settings.dbos_max_recovery_attempts
        self._client = client or DBOSClient(
            system_database_url=settings.dbos_system_database_url.get_secret_value(),
            dbos_system_schema=settings.dbos_system_schema,
            application_name=settings.service_name,
            retry_connection_errors=False,
        )

    def enqueue(self, job: CalculationJob) -> str:
        workflow_id = calculation_workflow_id(job.request.request_id, job.request.calc_process)
        options: EnqueueOptions = {
            "workflow_name": CALCULATION_WORKFLOW_NAME,
            "class_name": CALCULATION_WORKFLOW_CLASS,
            "instance_name": CALCULATION_WORKFLOW_INSTANCE,
            "queue_name": CALCULATION_QUEUE_NAME,
            "workflow_id": workflow_id,
            "app_version": self._application_version,
            "max_recovery_attempts": self._max_recovery_attempts,
        }
        self._client.enqueue(options, job.model_dump(mode="json"))
        return workflow_id

    def close(self) -> None:
        self._client.destroy()


def calculation_workflow_id(request_id: str, calc_process: str) -> str:
    return str(uuid5(WORKFLOW_ID_NAMESPACE, f"{request_id}:{calc_process}"))
