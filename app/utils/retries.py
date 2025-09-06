import time
from typing import Any, Callable, Iterable, TypeVar

T = TypeVar("T")


def retry_call(
    fn: Callable[..., T],
    *args: Any,
    retries: int = 3,
    base_delay: float = 0.5,
    **kwargs: Any,
) -> T:
    """
    Exponential backoff retry helper for functions.
    Raises the last exception after exhausting retries.
    """
    attempt = 0
    while True:
        try:
            return fn(*args, **kwargs)
        except Exception:
            attempt += 1
            if attempt > retries:
                raise
            time.sleep(base_delay * (2 ** (attempt - 1)))


def retry_iter(
    fn: Callable[..., Iterable[T]],
    *args: Any,
    retries: int = 3,
    base_delay: float = 0.5,
    **kwargs: Any,
) -> Iterable[T]:
    """
    Retry helper for iterator-returning LLM helpers.
    Materializes once with retry, then yields results.
    """
    items = retry_call(fn, *args, retries=retries, base_delay=base_delay, **kwargs)
    for it in items:
        yield it
