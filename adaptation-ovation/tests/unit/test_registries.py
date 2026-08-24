from collections.abc import Mapping
from typing import Any

import pytest

from calculation_worker.application.registries import MessageHandlerRegistry
from calculation_worker.calculations.registry import CalculationRegistry
from calculation_worker.domain.models import (
    CalculationRequested,
    CalculationResult,
    HandlerResult,
    MessageContext,
)
from calculation_worker.errors import (
    DuplicateCalculationHandlerError,
    DuplicateMessageHandlerError,
    UnsupportedCalculationError,
)


class StubMessageHandler:
    def __init__(self, event_type: str) -> None:
        self.event_type = event_type

    def handle(
        self,
        payload: Mapping[str, Any],
        context: MessageContext,
    ) -> HandlerResult:
        return HandlerResult(records=(), outcome="handled")


class StubCalculation:
    def __init__(self, process_name: str) -> None:
        self.process_name = process_name

    def calculate(self, request: CalculationRequested) -> CalculationResult:
        return CalculationResult(data={"request_id": request.request_id})


def test_message_handler_can_be_registered() -> None:
    registry = MessageHandlerRegistry()
    handler = StubMessageHandler("example.event")

    registry.register(handler)

    assert registry.get("example.event") is handler


def test_message_handler_can_be_retrieved_by_event_type() -> None:
    registry = MessageHandlerRegistry()
    first = StubMessageHandler("first.event")
    second = StubMessageHandler("second.event")
    registry.register(first)
    registry.register(second)

    assert registry.get("second.event") is second


def test_duplicate_message_handler_is_rejected() -> None:
    registry = MessageHandlerRegistry()
    registry.register(StubMessageHandler("duplicate.event"))

    with pytest.raises(DuplicateMessageHandlerError, match=r"duplicate\.event"):
        registry.register(StubMessageHandler("duplicate.event"))


def test_unknown_event_type_has_no_handler() -> None:
    assert MessageHandlerRegistry().get("future.event") is None


def test_calculation_can_be_registered() -> None:
    registry = CalculationRegistry()
    calculation = StubCalculation("example")

    registry.register(calculation)

    assert registry.get("example") is calculation


def test_calculation_can_be_retrieved_by_process_name() -> None:
    registry = CalculationRegistry()
    first = StubCalculation("first")
    second = StubCalculation("second")
    registry.register(first)
    registry.register(second)

    assert registry.get("second") is second


def test_duplicate_calculation_is_rejected() -> None:
    registry = CalculationRegistry()
    registry.register(StubCalculation("duplicate"))

    with pytest.raises(DuplicateCalculationHandlerError, match="duplicate"):
        registry.register(StubCalculation("duplicate"))


def test_unknown_calculation_raises_explicit_error() -> None:
    with pytest.raises(UnsupportedCalculationError, match="missing") as captured:
        CalculationRegistry().get("missing")

    assert captured.value.process_name == "missing"
