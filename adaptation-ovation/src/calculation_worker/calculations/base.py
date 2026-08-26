"""Calculation handler contract."""

from collections.abc import Mapping
from typing import Any, Protocol

from calculation_worker.domain.models import CalculationRequested, CalculationResult


class CalculationHandler(Protocol):
    """Perform one named calculation independently of Kafka."""

    process_name: str

    def fetch_input(self, request: CalculationRequested) -> Mapping[str, Any]:
        """Fetch external input for a validated request."""
        ...

    def calculate(
        self,
        request: CalculationRequested,
        input_data: Mapping[str, Any],
    ) -> CalculationResult:
        """Calculate a result from already fetched input."""
        ...
