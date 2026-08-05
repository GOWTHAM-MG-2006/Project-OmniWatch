"""
OmniWatch — Orchestration + Policy
Component: Orchestration Producer
Phase: 9
Purpose: Kafka producer that publishes ActionResult records to the
         omniwatch.remediation.actions topic for downstream consumers
         (dashboard, learning loop, compliance reporter).
Inputs: ActionResult dict or Pydantic model
Outputs: Kafka messages on omniwatch.remediation.actions (JSON)
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Optional

from ingestion.kafka_bus import KafkaProducer, TOPIC_REMEDIATION_ACTIONS
from orchestration.config.settings import Settings
from storage.common import create_logger

_LOG: logging.Logger = create_logger("omniwatch.orchestration.orchestration_producer")

# ---------------------------------------------------------------------------
# Retry constants — exponential backoff: 500ms → 2.5s → 12.5s
# Matches the decision_client._call_opa_with_retry pattern but with base
# delay tuned so delays land at exactly 0.5, 2.5, 12.5 seconds.
# Formula: delay = min(base_delay * MULTIPLIER^attempt, max_delay)
#   attempt 0: min(0.5 * 5^0, 12.5) = 0.5
#   attempt 1: min(0.5 * 5^1, 12.5) = 2.5
#   attempt 2: min(0.5 * 5^2, 12.5) = 12.5
# ---------------------------------------------------------------------------
_RETRY_MULTIPLIER: float = 5.0
_RETRY_BASE_DELAY: float = 0.5
_RETRY_MAX_DELAY: float = 12.5
_RETRY_ATTEMPTS: int = 3  # retries after initial attempt (4 total calls)

# Required identity keys in the ActionResult dict — publish is skipped
# if any of these are missing.
_REQUIRED_ACTION_RESULT_KEYS: frozenset[str] = frozenset({
    "action_type",
    "incident_id",
    "entity_id",
    "success",
})


def _to_dict(action_result: Any) -> dict[str, Any]:
    """Convert an ActionResult to a plain dict.

    Accepts a Pydantic v2 model (uses ``model_dump()``) or a plain dict.
    Datetime objects are serialized to ISO8601 strings via the JSON default
    handler so the payload is always JSON-safe.
    """
    if hasattr(action_result, "model_dump"):
        data = action_result.model_dump()
    elif isinstance(action_result, dict):
        data = dict(action_result)
    else:
        raise TypeError(
            f"Expected dict or Pydantic model, got {type(action_result).__name__}"
        )
    # Ensure datetime fields are JSON-serializable strings
    return json.loads(json.dumps(data, default=str))


class OrchestrationProducer:
    """Kafka producer for the ``omniwatch.remediation.actions`` topic.

    Wraps the shared ``ingestion.kafka_bus.KafkaProducer`` and handles
    serialization of ActionResult records plus retry-on-failure with
    exponential backoff (500ms → 2.5s → 12.5s, matching the
    decision_client._call_opa_with_retry pattern).

    Args:
        settings: Optional Settings; defaults to ``Settings()`` from env.
        bootstrap_servers: Optional override for Kafka bootstrap servers.
        client_id: Optional override for Kafka client id.
    """

    def __init__(
        self,
        settings: Optional[Settings] = None,
        bootstrap_servers: Optional[str] = None,
        client_id: Optional[str] = None,
    ) -> None:
        self._settings = settings or Settings()
        self._bootstrap_servers = (
            bootstrap_servers or self._settings.kafka_bootstrap_servers
        )
        self._client_id = client_id or "omniwatch-orchestration"
        self._producer: Optional[KafkaProducer] = None

    @property
    def topic(self) -> str:
        """Return the produced topic name."""
        return TOPIC_REMEDIATION_ACTIONS

    def start(self) -> None:
        """Initialize the Kafka producer."""
        self._producer = KafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            client_id=f"{self._client_id}-producer",
        )
        self._producer.start()
        _LOG.info(
            "orchestration producer started: client=%s topic=%s",
            self._client_id,
            TOPIC_REMEDIATION_ACTIONS,
        )

    def stop(self, timeout: float = 5.0) -> None:
        """Flush and stop the Kafka producer."""
        if self._producer is not None:
            self._producer.flush(timeout=timeout)
            self._producer.stop(timeout=timeout)
            self._producer = None
        _LOG.info("orchestration producer stopped")

    def close(self, timeout: float = 5.0) -> None:
        """Alias for ``stop()`` — flush and release resources."""
        self.stop(timeout=timeout)

    def publish_action_result(
        self,
        action_result: dict[str, Any],
        key: Optional[str] = None,
    ) -> bool:
        """Publish an ActionResult record to Kafka.

        Accepts either a Pydantic model (with ``model_dump()``) or a plain
        dict.  Serializes to JSON and sends to ``omniwatch.remediation.actions``.

        Retries transient failures with exponential backoff
        (500ms → 2.5s → 12.5s, 3 retries = 4 total attempts).

        Args:
            action_result: ActionResult as a dict or Pydantic model.
            key: Optional message key (defaults to ``action_id``).

        Returns:
            ``True`` on successful delivery, ``False`` after retries exhausted.
        """
        if self._producer is None:
            _LOG.error("publish_action_result called but producer not started")
            return False

        # --- Serialize to plain dict ---
        try:
            data = _to_dict(action_result)
        except (TypeError, ValueError) as exc:
            _LOG.warning("failed to serialize action_result: %s", exc)
            return False

        # --- Validate required identity fields ---
        missing = _REQUIRED_ACTION_RESULT_KEYS - data.keys()
        if missing:
            _LOG.warning(
                "action_result missing required keys: %s — skipping publish",
                sorted(missing),
            )
            return False

        msg_key = key or data.get("action_id", data.get("incident_id", "unknown"))
        last_error: Optional[Exception] = None
        total_attempts = _RETRY_ATTEMPTS + 1

        for attempt in range(total_attempts):
            delivery_error: Optional[str] = None

            def _on_delivery(
                _err_key: str, error_msg: str
            ) -> None:
                nonlocal delivery_error
                if error_msg:
                    delivery_error = error_msg

            try:
                self._producer.send(
                    TOPIC_REMEDIATION_ACTIONS,
                    data,
                    key=msg_key,
                    callback=_on_delivery,
                )
                remaining = self._producer.flush(timeout=10.0)

                # Check for delivery errors or unsent messages
                if delivery_error is None and remaining == 0:
                    _LOG.debug(
                        "published action_result: action_id=%s topic=%s",
                        data.get("action_id"),
                        TOPIC_REMEDIATION_ACTIONS,
                    )
                    return True

                err_msg = delivery_error or f"{remaining} messages still in queue"
                raise RuntimeError(f"delivery failed: {err_msg}")

            except Exception as exc:
                last_error = exc
                if attempt < _RETRY_ATTEMPTS:
                    wait = min(
                        _RETRY_BASE_DELAY * (_RETRY_MULTIPLIER ** attempt),
                        _RETRY_MAX_DELAY,
                    )
                    _LOG.warning(
                        "attempt %d/%d failed: %s; retrying in %.1fs",
                        attempt + 1,
                        total_attempts,
                        exc,
                        wait,
                    )
                    time.sleep(wait)

        _LOG.error(
            "publish_action_result failed after %d attempts: action_id=%s error=%s",
            total_attempts,
            data.get("action_id"),
            last_error,
        )
        return False

    def flush(self, timeout: float = 5.0) -> int:
        """Flush pending messages. Returns remaining count."""
        if self._producer is None:
            return 0
        return self._producer.flush(timeout=timeout)
