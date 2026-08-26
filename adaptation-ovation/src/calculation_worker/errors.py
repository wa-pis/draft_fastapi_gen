"""Typed errors shared by the worker's application and adapters."""


class CalculationWorkerError(Exception):
    """Base class for expected worker errors."""


class DuplicateMessageHandlerError(CalculationWorkerError, ValueError):
    """Raised when an event type is registered more than once."""

    def __init__(self, event_type: str) -> None:
        self.event_type = event_type
        super().__init__(f"Message handler already registered: {event_type!r}")


class DuplicateCalculationHandlerError(CalculationWorkerError, ValueError):
    """Raised when a calculation process is registered more than once."""

    def __init__(self, process_name: str) -> None:
        self.process_name = process_name
        super().__init__(f"Calculation handler already registered: {process_name!r}")


class UnsupportedCalculationError(CalculationWorkerError, LookupError):
    """Raised when no calculation is registered for a process name."""

    def __init__(self, process_name: str) -> None:
        self.process_name = process_name
        super().__init__(f"Unsupported calculation: {process_name!r}")


class UpstreamError(CalculationWorkerError):
    """Base class for terminal upstream API errors."""


class UpstreamTimeoutError(UpstreamError):
    """The single upstream API attempt timed out."""


class UpstreamNetworkError(UpstreamError):
    """The single upstream API attempt failed at the network layer."""


class UpstreamHttpError(UpstreamError):
    """The upstream API returned an unsuccessful HTTP response."""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"Upstream returned HTTP status {status_code}")


class InvalidUpstreamResponseError(UpstreamError):
    """The upstream response was not a valid JSON object."""


class PublishError(CalculationWorkerError):
    """One or more outgoing Kafka records were not acknowledged."""


class ConsumeError(CalculationWorkerError):
    """The Kafka consumer failed to poll or commit a message."""


class ConfigurationError(CalculationWorkerError):
    """The worker cannot safely start with its current configuration."""


class TerminalCalculationError(CalculationWorkerError):
    """A sanitized terminal error used as the persisted DBOS workflow outcome."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"Calculation workflow failed: {code}")
