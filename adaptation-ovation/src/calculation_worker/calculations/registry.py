"""Explicit registry for calculation handlers."""

from calculation_worker.calculations.base import CalculationHandler
from calculation_worker.errors import (
    DuplicateCalculationHandlerError,
    UnsupportedCalculationError,
)


class CalculationRegistry:
    """Map calculation process names to explicitly registered handlers."""

    def __init__(self) -> None:
        self._handlers: dict[str, CalculationHandler] = {}

    def register(self, handler: CalculationHandler) -> None:
        """Register a calculation, rejecting duplicate process names."""
        if handler.process_name in self._handlers:
            raise DuplicateCalculationHandlerError(handler.process_name)
        self._handlers[handler.process_name] = handler

    def get(self, process_name: str) -> CalculationHandler:
        """Return the selected calculation or raise an explicit domain error."""
        try:
            return self._handlers[process_name]
        except KeyError:
            raise UnsupportedCalculationError(process_name) from None
