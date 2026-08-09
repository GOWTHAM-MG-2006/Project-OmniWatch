"""
OmniWatch — Predictive Intelligence Layer
Component: Anomaly Session Tracker
Phase: 6
Purpose: Track active anomaly sessions per (entity_id, metric_name) and resolve
         them after a run of consecutive normal scores
Inputs: Anomaly scores with entity_id + metric_name + ISO-8601 timestamp
Outputs: AnomalySession dicts (active / resolved) for downstream duration tracking
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# ─── Constants ────────────────────────────────────────────────────────────── #

_DEFAULT_THRESHOLD = 0.5
_DEFAULT_RESOLUTION_WINDOW = 3


# ─── Helpers ──────────────────────────────────────────────────────────────── #

def _parse_timestamp(ts: Any) -> Optional[datetime]:
    """Coerce a timestamp into a timezone-aware datetime, or ``None``.

    Accepts ISO-8601 strings (with or without a trailing ``Z``), epoch
    seconds (int/float), or an existing ``datetime``.  Returns ``None`` when
    the value cannot be parsed so callers can degrade gracefully.
    """
    if isinstance(ts, datetime):
        return ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)
    if isinstance(ts, (int, float)):
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (ValueError, OverflowError, OSError):
            return None
    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def _elapsed_seconds(start_ts: Any, end_ts: Any) -> float:
    """Wall-clock seconds between two timestamps (0.0 when unparseable)."""
    start_dt = _parse_timestamp(start_ts)
    end_dt = _parse_timestamp(end_ts)
    if start_dt is None or end_dt is None:
        return 0.0
    return max(0.0, (end_dt - start_dt).total_seconds())


# ─── AnomalySession ───────────────────────────────────────────────────────── #

@dataclass
class AnomalySession:
    """A single continuous anomaly episode for one (entity_id, metric_name).

    Attributes
    ----------
    entity_id : str
        The entity the anomaly was observed on.
    metric_name : str
        The metric that triggered the anomaly.
    start_time : str
        ISO-8601 timestamp of the first anomalous observation.
    last_update : str
        ISO-8601 timestamp of the most recent observation.
    peak_score : float
        Highest anomaly score seen during the session.
    score_history : list[float]
        Every score observed during the session, in order.
    resolution_status : str
        ``"active"`` while the session is ongoing, ``"resolved"`` once the
        3-consecutive-normal rule has fired.
    duration_seconds : float
        Wall-clock seconds between ``start_time`` and ``last_update``.
    """

    entity_id: str
    metric_name: str
    start_time: str
    last_update: str
    peak_score: float
    score_history: List[float] = field(default_factory=list)
    resolution_status: str = "active"
    duration_seconds: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serialisable dict representation of the session."""
        return {
            "entity_id": self.entity_id,
            "metric_name": self.metric_name,
            "start_time": self.start_time,
            "last_update": self.last_update,
            "peak_score": self.peak_score,
            "score_history": list(self.score_history),
            "resolution_status": self.resolution_status,
            "duration_seconds": self.duration_seconds,
        }


# ─── AnomalySessionTracker ────────────────────────────────────────────────── #

class AnomalySessionTracker:
    """Tracks active anomaly sessions keyed by ``(entity_id, metric_name)``.

    A session starts on the first anomalous observation and is updated on
    every subsequent observation.  When ``check_resolution`` observes
    ``resolution_window`` consecutive scores below ``threshold``, the session
    is marked ``"resolved"`` and moved from the active map to the resolved
    list, so a later anomaly for the same key starts a fresh session.
    """

    def __init__(
        self,
        threshold: float = _DEFAULT_THRESHOLD,
        resolution_window: int = _DEFAULT_RESOLUTION_WINDOW,
    ) -> None:
        self.threshold = threshold
        self.resolution_window = resolution_window
        self._active: Dict[Tuple[str, str], AnomalySession] = {}
        self._resolved: List[AnomalySession] = []

    # ── public API ──────────────────────────────────────────────────── #

    def start(self, entity_id: str, metric_name: str, score: float, timestamp: str) -> AnomalySession:
        """Begin (or continue) an anomaly session for the given key.

        Returns the session.  When an active session already exists for the
        key, the observation is folded into it via :meth:`update` and the
        existing session is returned; otherwise a fresh session is created.
        """
        key = (entity_id, metric_name)
        if key in self._active:
            return self.update(entity_id, metric_name, score, timestamp)
        session = AnomalySession(
            entity_id=entity_id,
            metric_name=metric_name,
            start_time=timestamp,
            last_update=timestamp,
            peak_score=float(score),
            score_history=[float(score)],
            duration_seconds=0.0,
        )
        self._active[key] = session
        return session

    def update(self, entity_id: str, metric_name: str, score: float, timestamp: str) -> AnomalySession:
        """Record a new observation on an existing active session.

        Raises ``KeyError`` when no active session exists for the key.
        """
        key = (entity_id, metric_name)
        if key not in self._active:
            raise KeyError(f"No active anomaly session for {entity_id}/{metric_name}")
        session = self._active[key]
        score = float(score)
        session.score_history.append(score)
        if score > session.peak_score:
            session.peak_score = score
        session.last_update = timestamp
        session.duration_seconds = _elapsed_seconds(session.start_time, timestamp)
        return session

    def check_resolution(self, entity_id: str, metric_name: str, score: float, timestamp: str) -> bool:
        """Apply the consecutive-normal resolution rule.

        Records the observation, then resolves the session when the last
        ``resolution_window`` scores are all strictly below ``threshold``.
        Returns ``True`` when the session was resolved, else ``False``.
        """
        session = self.update(entity_id, metric_name, score, timestamp)
        if session.resolution_status == "resolved":
            return True
        recent = session.score_history[-self.resolution_window :]
        if len(recent) >= self.resolution_window and all(s < self.threshold for s in recent):
            session.resolution_status = "resolved"
            self._active.pop((entity_id, metric_name), None)
            self._resolved.append(session)
            return True
        return False

    def get_session(self, entity_id: str, metric_name: str) -> Optional[AnomalySession]:
        """Return the active session for the key, or ``None``."""
        return self._active.get((entity_id, metric_name))

    def get_active_sessions(self) -> Dict[Tuple[str, str], AnomalySession]:
        """Return a copy of the active-session map keyed by (entity_id, metric_name)."""
        return dict(self._active)

    def get_resolved_sessions(self) -> List[AnomalySession]:
        """Return a copy of the list of resolved sessions."""
        return list(self._resolved)