"""Explicit registry for Kafka message handlers."""

from calculation_worker.application.contracts import MessageHandler
from calculation_worker.errors import DuplicateMessageHandlerError


class MessageHandlerRegistry:
    """Map event types to explicitly registered message handlers."""

    def __init__(self) -> None:
        self._handlers: dict[str, MessageHandler] = {}

    def register(self, handler: MessageHandler) -> None:
        """Register a handler, rejecting ambiguous duplicate routes."""
        if handler.event_type in self._handlers:
            raise DuplicateMessageHandlerError(handler.event_type)
        self._handlers[handler.event_type] = handler

    def get(self, event_type: str) -> MessageHandler | None:
        """Return a handler or ``None`` for a forward-compatible event."""
        return self._handlers.get(event_type)
