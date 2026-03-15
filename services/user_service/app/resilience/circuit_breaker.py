import time
from enum import Enum
from collections.abc import Awaitable, Callable
from typing import TypeVar


T = TypeVar("T")


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, reset_timeout: float = 30.0) -> None:
        self.state = CircuitState.CLOSED
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self._failure_count = 0
        self._opened_at: float | None = None

    async def call(self, func: Callable[..., Awaitable[T]], *args, **kwargs) -> T:
        if self.state is CircuitState.OPEN:
            assert self._opened_at is not None
            if time.time() - self._opened_at > self.reset_timeout:
                self.state = CircuitState.HALF_OPEN
            else:
                raise RuntimeError("Circuit breaker open")

        try:
            result = await func(*args, **kwargs)
        except Exception:
            self._failure_count += 1
            if self._failure_count >= self.failure_threshold:
                self.state = CircuitState.OPEN
                self._opened_at = time.time()
            raise
        else:
            if self.state in (CircuitState.HALF_OPEN, CircuitState.OPEN):
                self.state = CircuitState.CLOSED
            self._failure_count = 0
            return result

