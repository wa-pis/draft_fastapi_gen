from collections.abc import Mapping
from typing import Any, Protocol

from calculation_worker.domain.models import CalculationRequested, CalculationResult


class UpstreamDataProvider(Protocol):
    def get_data(self, calc_id: str) -> Mapping[str, Any]: ...


class ExampleCalculation:
    process_name = "example"

    def __init__(self, upstream_client: UpstreamDataProvider) -> None:
        self._upstream_client = upstream_client

    def calculate(self, request: CalculationRequested) -> CalculationResult:
        source_data = self._upstream_client.get_data(request.calc_id)
        return CalculationResult(
            data={
                "has_data": True,
                "source_field_count": len(source_data),
            }
        )
