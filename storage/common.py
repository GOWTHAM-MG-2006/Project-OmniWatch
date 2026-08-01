"""
OmniWatch — Unified Storage Layer
Component: Common Utilities
Phase: 5
Purpose: Shared structured-JSON logging setup, the StorageError exception
         base, and a 3x exponential-backoff retry helper for storage clients.
Inputs: Python logging records; callables (client connect/query functions)
Outputs: JSON log lines to stdout; retried function results, or the last
         exception raised after retries are exhausted
"""

from __future__ import annotations

import json
import logging
import sys
import time
from typing import Any, Callable, Optional

# 3x exponential backoff between retries (initial attempt + 3 retries = 4 total
# attempts, matching the plan's "retry 3x 100ms -> 500ms -> 2s" contract).
# delay(attempt) = base_delay * 5 ** attempt capped at max_delay:
#   0.1s -> 0.5s -> 2.0s
RETRY_MULTIPLIER: float = 5.0


class StorageError(Exception):
    """Base exception for all Unified Storage Layer (Phase 5) components."""


class _JsonLogFormatter(logging.Formatter):
    """Structured JSON log formatter (AGENTS.md: stdout logs as JSON)."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def create_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Return a logger emitting structured JSON lines to stdout.

    Idempotent per logger name: a logger that already has handlers is not
    double-configured, so repeated calls from the same component are safe.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonLogFormatter())
        logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    return logger


def retry_with_backoff(
    func: Callable[..., Any],
    retries: int = 3,
    base_delay: float = 0.1,
    max_delay: float = 2.0,
    logger: Optional[logging.Logger] = None,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Invoke ``func``, retrying transient failures with 3x backoff.

    Sleeps ``base_delay * 5 ** attempt`` (capped at ``max_delay``) between
    attempts — the defaults produce 100ms -> 500ms -> 2s. ``retries`` is the
    number of retries after the initial attempt (so ``retries + 1`` total
    calls). Each retry is logged at WARNING; after retries are exhausted the
    last exception is re-raised.
    """
    log = logger or create_logger("omniwatch.storage.common")
    last_error: Optional[Exception] = None
    total_attempts = retries + 1
    for attempt in range(total_attempts):
        try:
            return func(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - transient failures retry
            last_error = exc
            if attempt < retries:
                wait = min(base_delay * (RETRY_MULTIPLIER ** attempt), max_delay)
                log.warning(
                    "attempt %d/%d failed: %s; retrying in %.1fs",
                    attempt + 1,
                    total_attempts,
                    exc,
                    wait,
                )
                time.sleep(wait)
    # Unreachable in practice: the loop body always ran at least once.
    assert last_error is not None
    raise last_error
