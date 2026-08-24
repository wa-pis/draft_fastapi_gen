"""Calculation handler contract."""

from typing import Protocol

from calculation_worker.domain.models import CalculationRequested, CalculationResult


class CalculationHandler(Protocol):
    """Perform one named calculation independently of Kafka."""

    process_name: str

    def calculate(self, request: CalculationRequested) -> CalculationResult:
        """Calculate a result for a validated request."""
        ...
