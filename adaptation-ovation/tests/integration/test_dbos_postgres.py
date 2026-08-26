from __future__ import annotations

from typing import Any, cast

import docker
import pytest
from dbos import DBOSClient, run_dbos_database_migrations
from testcontainers.community.postgres import PostgresContainer

from calculation_worker.domain.models import CalculationJob, CalculationRequested
from calculation_worker.infrastructure.dbos_queue import DBOSCalculationQueue
from calculation_worker.settings import Settings

pytestmark = pytest.mark.integration


@pytest.fixture
def postgres() -> Any:
    try:
        client = docker.from_env()
        client.ping()
        client.close()
    except Exception:
        pytest.skip("Docker is not available")
    with PostgresContainer("postgres:16-alpine", driver=None) as container:
        yield container


def test_duplicate_logical_request_keeps_one_workflow_and_first_payload(
    postgres: PostgresContainer,
) -> None:
    database_url = postgres.get_connection_url()
    run_dbos_database_migrations(system_database_url=database_url, schema="dbos")
    settings = Settings(
        _env_file=None,
        DBOS_SYSTEM_DATABASE_URL=database_url,
        DBOS_APPLICATION_VERSION="integration-v1",
        UPSTREAM_API_BASE_URL="http://unused.test",
        UPSTREAM_API_TOKEN="unused",
    )
    native = DBOSClient(
        system_database_url=database_url,
        dbos_system_schema="dbos",
        application_name=settings.service_name,
        retry_connection_errors=False,
    )
    queue = DBOSCalculationQueue(settings, native)
    first = _job("calc-first")
    second = _job("calc-second")

    first_id = queue.enqueue(first)
    second_id = queue.enqueue(second)
    statuses = native.list_workflows(workflow_ids=[first_id], load_input=True)

    assert first_id == second_id
    assert len(statuses) == 1
    assert statuses[0]["status"] == "ENQUEUED"
    inputs = cast(dict[str, Any], statuses[0]["input"])
    assert inputs["args"][0]["request"]["calc_id"] == "calc-first"
    queue.close()


def _job(calc_id: str) -> CalculationJob:
    return CalculationJob.create(
        CalculationRequested(
            event_type="calculation.requested",
            schema_version=1,
            request_id="same-request",
            calc_id=calc_id,
            calc_process="example",
        )
    )
