"""
OmniWatch — Incident Prioritization
Component: Deduplication Engine (GAP 3)
Phase: 8
Purpose: Prevent alert storms by grouping anomalies with the same root cause
         into a single incident within a TTL window. Uses an in-memory
         thread-safe TTLCache for single-host operation.
Inputs: IncidentRecord (after initial creation, before Kafka publish)
Outputs: IncidentRecord (updated with deduplicated_count, possibly new incident_id)
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Optional

from prioritization.models import IncidentRecord
from storage.common import create_logger

_LOG: logging.Logger = create_logger("omniwatch.prioritization.deduplication_engine")

# Dedup key template: root_cause_entity
# TTL window: 300 seconds (5 minutes) — matches dedup_ttl_seconds from settings


class DeduplicationEngine:
    """Thread-safe in-memory alert deduplication engine.

    Groups incidents that share the same ``root_cause_entity`` within a
    TTL window (default 300 s / 5 min).  When a duplicate is detected the
    existing incident's ``deduplicated_count`` is incremented and the
    incoming incident is merged — only the original ``incident_id`` and
    ``root_cause`` are kept; new evidence is appended via ``related_anomalies``.

    Known limitation (documented in README): single-host only.  Horizontal
    scaling requires migration to Redis-based shared state.

    Args:
        ttl_seconds: Time-to-live for dedup entries (default 300).
        enabled: Master toggle — when False, every incident passes through
            unchanged (deduplicated_count = 1, status = "OPEN").
    """

    def __init__(self, ttl_seconds: int = 300, enabled: bool = True) -> None:
        self._ttl = ttl_seconds
        self._enabled = enabled
        self._cache: dict[str, tuple[float, IncidentRecord]] = {}
        self._lock = threading.RLock()

    @property
    def ttl_seconds(self) -> int:
        return self._ttl

    @property
    def enabled(self) -> bool:
        return self._enabled

    def _cache_key(self, incident: IncidentRecord) -> str:
        """Build the dedup key from the root cause entity.

        Falls back to the incident_id when root_cause_entity is empty
        so every dedup key is unique and no incident is lost.
        """
        entity = incident.root_cause.root_cause_entity or incident.incident_id
        return f"{entity}"

    def _evict_expired(self) -> None:
        """Remove all expired entries from the cache (caller must hold _lock)."""
        now = time.monotonic()
        expired = [
            key for key, (ts, _) in self._cache.items()
            if now - ts > self._ttl
        ]
        for key in expired:
            del self._cache[key]
        if expired:
            _LOG.debug("evicted %d expired dedup entries", len(expired))

    def check_and_dedup(self, incident: IncidentRecord) -> IncidentRecord:
        """Check if *incident* is a duplicate; merge if so, return updated record.

        If the engine is disabled, the incident is returned unchanged
        (with ``deduplicated_count = 1``).

        Returns:
            The incident to publish.  May be the original (new incident)
            with ``deduplicated_count = 1``, or an existing incident with an
            incremented ``deduplicated_count`` and merged evidence.
        """
        if not self._enabled:
            return incident

        with self._lock:
            self._evict_expired()
            key = self._cache_key(incident)

            existing = self._cache.get(key)
            if existing is None:
                # New incident — cache it
                self._cache[key] = (time.monotonic(), incident)
                _LOG.info(
                    "new incident cached: key=%s incident_id=%s",
                    key,
                    incident.incident_id,
                )
                return incident

            ts, existing_incident = existing
            if time.monotonic() - ts > self._ttl:
                # Expired — treat as new
                del self._cache[key]
                self._cache[key] = (time.monotonic(), incident)
                _LOG.info(
                    "dedup entry expired, creating new incident: key=%s incident_id=%s",
                    key,
                    incident.incident_id,
                )
                return incident

            # Duplicate — merge into the existing incident
            merged = self._merge_duplicate(existing_incident, incident)
            # Update cache with fresh timestamp (sliding window)
            self._cache[key] = (time.monotonic(), merged)
            _LOG.info(
                "deduplicated incident: key=%s original=%s count=%d",
                key,
                existing_incident.incident_id,
                merged.deduplicated_count,
            )
            return merged

    @staticmethod
    def _merge_duplicate(
        existing: IncidentRecord, incoming: IncidentRecord
    ) -> IncidentRecord:
        """Merge an incoming duplicate into the existing incident record.

        - Keeps the existing ``incident_id`` and ``created_at``
        - Increments ``deduplicated_count``
        - Appends incoming root_cause to ``related_anomalies``
        - Updates ``business_impact_score`` to the max of both
        """
        # Build the merged related_anomalies list
        related = list(existing.related_anomalies)
        incoming_rc = incoming.root_cause.model_dump() if hasattr(incoming.root_cause, "model_dump") else dict(incoming.root_cause) if isinstance(incoming.root_cause, dict) else {}
        related.append(incoming_rc)

        # Keep the higher impact score
        new_impact = max(existing.business_impact_score, incoming.business_impact_score)

        # Create merged incident from the existing record
        merged = existing.model_copy(deep=True)
        merged.related_anomalies = related
        merged.deduplicated_count = existing.deduplicated_count + 1
        merged.business_impact_score = new_impact

        return merged

    def get_stats(self) -> dict[str, Any]:
        """Return current cache statistics (for monitoring / tests)."""
        with self._lock:
            self._evict_expired()
            return {
                "enabled": self._enabled,
                "ttl_seconds": self._ttl,
                "cached_incidents": len(self._cache),
                "keys": list(self._cache.keys()),
            }

    def clear(self) -> None:
        """Clear the dedup cache (used in tests)."""
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            _LOG.debug("dedup cache cleared (%d entries evicted)", count)
