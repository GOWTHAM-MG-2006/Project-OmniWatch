"""
OmniWatch — Predictive Intelligence Layer
Component: Noise Filter
Phase: 6
Purpose: Transient spike suppression with cascade awareness
Inputs: Anomaly signals with timestamps
Outputs: Suppression decision (bool)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Tuple

# A spike is considered transient if it lasts less than this duration.
SPIKE_DURATION_THRESHOLD_SECONDS = 180  # 3 minutes

# A cascade is declared when this many neighbors are affected.
CASCADE_NEIGHBOR_THRESHOLD = 3

# Anomalies at or above this score are never suppressed.
CRITICAL_SCORE_THRESHOLD = 0.85


class NoiseFilter:
    """Suppresses transient, isolated spikes while passing cascades and
    high-confidence anomalies through.

    Tracks the first-seen timestamp per (entity_id, metric) so that the
    duration of a spike can be measured. A spike is suppressed only when it
    is short-lived AND affects few neighbors. Cascades (>= 3 neighbors),
    long-running spikes, and security signals always pass through.
    """

    def __init__(self) -> None:
        # (entity_id, metric) -> first_seen_timestamp (datetime, UTC)
        self._first_seen: Dict[Tuple[str, str], datetime] = {}

    def should_suppress(
        self,
        entity_id: str,
        metric: str,
        timestamp: datetime,
        affected_neighbors: int = 0,
        source_type: str = "performance",
        anomaly_score: float = 0.0,
    ) -> bool:
        """Return True to suppress the anomaly, False to pass it through.

        Args:
            entity_id: The entity the anomaly was detected on.
            metric: The metric name that is anomalous.
            timestamp: When the anomaly was observed (UTC).
            affected_neighbors: Number of neighboring entities also affected.
            source_type: "performance" or "security".
            anomaly_score: Confidence score in [0.0, 1.0].

        Returns:
            True if the spike should be suppressed as transient noise,
            False if it should pass through to downstream processing.
        """
        # Critical bypass: never suppress high-confidence anomalies.
        if anomaly_score >= CRITICAL_SCORE_THRESHOLD:
            return False

        # Security signals always pass through.
        if source_type == "security":
            return False

        # Cascade: >= 3 affected neighbors means a real incident, not noise.
        if affected_neighbors >= CASCADE_NEIGHBOR_THRESHOLD:
            return False

        # Track first-seen timestamp for this (entity_id, metric).
        key = (entity_id, metric)
        if key not in self._first_seen:
            self._first_seen[key] = timestamp

        first_seen = self._first_seen[key]
        duration = (timestamp - first_seen).total_seconds()

        # Suppress only short-lived, isolated spikes.
        if duration < SPIKE_DURATION_THRESHOLD_SECONDS:
            return True

        return False

    def reset(self) -> None:
        """Clear all tracked first-seen timestamps."""
        self._first_seen.clear()