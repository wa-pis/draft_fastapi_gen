import asyncio
import random
from collections.abc import Callable, Awaitable
from typing import TypeVar


T = TypeVar("T")


async def retry_with_backoff(
    func: Callable[..., Awaitable[T]],
    *args,
    retries: int = 5,
    base_delay: float = 0.1,
    max_delay: float = 5.0,
    jitter: bool = True,
    **kwargs,
) -> T:
    attempt = 0
    while True:
        try:
            return await func(*args, **kwargs)
        except Exception:
            attempt += 1
            if attempt > retries:
                raise
            delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
            if jitter:
                delay = random.uniform(0, delay)
            await asyncio.sleep(delay)

