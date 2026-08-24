"""Application-facing structural interfaces."""

from collections.abc import Mapping
from typing import Any, Protocol

from calculation_worker.domain.models import HandlerResult, MessageContext


class MessageHandler(Protocol):
    """Handle one routed Kafka event without publishing or committing it."""

    event_type: str

    def handle(
        self,
        payload: Mapping[str, Any],
        context: MessageContext,
    ) -> HandlerResult:
        """Return every outgoing record required for this input."""
        ...
