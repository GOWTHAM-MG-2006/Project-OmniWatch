"""
OmniWatch — Predictive Intelligence Layer
Component: Evidence Aggregator
Phase: 6
Purpose: Collects the last 5 matching log lines as evidence for security anomalies
Inputs: Security event dicts with entity_id, attack_type, and log/message
Outputs: List of up to 5 evidence log snippets per (entity_id, attack_type)
"""

from __future__ import annotations

from collections import deque
from typing import Any, Dict, List, Tuple

# Maximum number of evidence log lines retained per (entity, attack) key.
EVIDENCE_BUFFER_MAX = 5


class EvidenceAggregator:
    """Maintains an in-memory ring buffer of evidence log lines keyed by
    ``(entity_id, attack_type)``.

    When :meth:`collect` is called the event's log line is appended to the
    buffer for that key and the last *up to* 5 lines are returned.  Older
    entries are automatically evicted by the :class:`~collections.deque`
    ``maxlen``.

    Thread-safety note: this class is *not* inherently thread-safe.  Callers
    that share an instance across threads must wrap access with a lock.
    """

    def __init__(self, max_per_key: int = EVIDENCE_BUFFER_MAX) -> None:
        self._max = max_per_key
        # (entity_id, attack_type) -> deque[str]
        self._buffers: Dict[Tuple[str, str], deque[str]] = {}

    # ── public API ──────────────────────────────────────────────────── #

    def collect(self, event: Dict[str, Any]) -> List[str]:
        """Append the event's log line to the ring buffer and return the
        last *up to* ``max_per_key`` evidence lines for that key.

        Parameters
        ----------
        event : dict
            Must contain at minimum ``entity_id`` and ``attack_type``.
            The log/message text is extracted from (in order of priority):
            ``"log"``, ``"message"``, or ``"description"``.  If none of
            these keys is present an empty string is used.

        Returns
        -------
        list[str]
            The current evidence lines (oldest → newest, length ≤ max_per_key).
        """
        entity_id = str(event.get("entity_id", ""))
        attack_type = str(event.get("attack_type", ""))
        log_line = self._extract_log_line(event)

        key = (entity_id, attack_type)
        buf = self._buffers.setdefault(key, deque(maxlen=self._max))
        buf.append(log_line)
        return list(buf)

    def get_evidence(self, entity_id: str, attack_type: str) -> List[str]:
        """Return the current evidence lines without appending anything.

        Useful for downstream consumers (e.g. SecuritySignalClassifier)
        that need to read the buffer without mutating it.
        """
        key = (entity_id, attack_type)
        buf = self._buffers.get(key)
        if buf is None:
            return []
        return list(buf)

    def clear(self, entity_id: str | None = None, attack_type: str | None = None) -> None:
        """Clear evidence buffers.

        If both *entity_id* and *attack_type* are ``None`` the entire
        state is reset.  Otherwise only matching keys are removed.
        """
        if entity_id is None and attack_type is None:
            self._buffers.clear()
            return

        keys_to_remove = [
            k for k in self._buffers
            if (entity_id is None or k[0] == entity_id)
            and (attack_type is None or k[1] == attack_type)
        ]
        for key in keys_to_remove:
            del self._buffers[key]

    # ── helpers ─────────────────────────────────────────────────────── #

    @staticmethod
    def _extract_log_line(event: Dict[str, Any]) -> str:
        """Extract a log/message string from the event dict."""
        for field in ("log", "message", "description"):
            value = event.get(field)
            if value is not None:
                return str(value)
        return ""
