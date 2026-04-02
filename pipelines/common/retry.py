from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, TypeVar

T = TypeVar("T")


def retry_with_backoff(
    operation: Callable[[], T],
    retries: int = 3,
    base_delay_seconds: float = 1.0,
    max_delay_seconds: float = 30.0,
    retry_exceptions: tuple[type[Exception], ...] = (Exception,),
    logger: Any | None = None,
    operation_name: str = "operation",
) -> T:
    """Retry an operation with exponential backoff."""

    attempt = 0
    while True:
        attempt += 1
        try:
            return operation()
        except retry_exceptions as exc:
            if attempt > retries:
                if logger:
                    logger.error(
                        "Retry limit exceeded",
                        extra={"operation": operation_name, "attempt": attempt, "error": str(exc)},
                    )
                raise

            delay = min(base_delay_seconds * (2 ** (attempt - 1)), max_delay_seconds)
            if logger:
                logger.warning(
                    "Operation failed, retrying",
                    extra={
                        "operation": operation_name,
                        "attempt": attempt,
                        "wait_seconds": delay,
                        "error": str(exc),
                    },
                )
            time.sleep(delay)
