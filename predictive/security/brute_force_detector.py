"""
OmniWatch — Predictive Intelligence Layer
Component: Brute Force Detector (GAP 1)
Phase: 6
Purpose: Detect brute force attacks by counting auth failure logs per (source_ip, 5min window)
Inputs: Security event dicts (from omniwatch.security.events Kafka topic)
Outputs: SecurityAnomalySignal for BRUTE_FORCE_ATTEMPT or None
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import yaml  # type: ignore[import-untyped]
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────── #

_DEFAULT_RULES_PATH = Path(__file__).resolve().parent.parent / "config" / "security_rules.yaml"

# Default thresholds (used when YAML is unavailable)
_DEFAULT_FAILURES_THRESHOLD = 10
_DEFAULT_WINDOW_MINUTES = 5
_DEFAULT_SEVERITY = "HIGH"


# ─── Helpers ──────────────────────────────────────────────────────────────── #

def _load_brute_force_rule(path: Path | None = None) -> dict[str, Any]:
    """Load brute_force rule from security_rules.yaml.

    Returns the ``brute_force`` section, or a minimal fallback dict when
    the file is missing or pyyaml is not installed.
    """
    fallback: dict[str, Any] = {
        "failures_threshold": _DEFAULT_FAILURES_THRESHOLD,
        "window_minutes": _DEFAULT_WINDOW_MINUTES,
        "severity": _DEFAULT_SEVERITY,
        "description": ">=10 failed auth attempts in 5 minutes",
    }

    rules_path = path or _DEFAULT_RULES_PATH
    if not rules_path.exists():
        logger.warning("security_rules.yaml not found at %s — using defaults", rules_path)
        return fallback

    if yaml is None:
        logger.warning("pyyaml not installed — using default brute_force rules")
        return fallback

    try:
        with open(rules_path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        attack_types = data.get("attack_types", {})
        bf_rule = attack_types.get("brute_force", {})
        if not bf_rule:
            logger.warning("brute_force rule missing from security_rules.yaml — using defaults")
            return fallback
        return bf_rule
    except Exception:
        logger.warning("Failed to parse security_rules.yaml — using defaults", exc_info=True)
        return fallback


# ─── Detector ─────────────────────────────────────────────────────────────── #

class BruteForceDetector:
    """Detect brute force attacks by counting auth failure events per source IP.

    Rule: if ``failures_threshold`` (default 10) or more auth-failure events
    from the same ``source_ip`` arrive within a ``window_minutes`` (default 5)
    sliding window, emit a ``SecurityAnomalySignal`` with
    ``attack_type="BRUTE_FORCE_ATTEMPT"``.

    Parameters
    ----------
    rules_path : Path | None
        Optional path to ``security_rules.yaml``.  ``None`` → default path.
    failures_threshold : int | None
        Override the failure count threshold.  ``None`` → read from YAML or default.
    window_minutes : int | None
        Override the time window.  ``None`` → read from YAML or default.
    """

    def __init__(
        self,
        rules_path: Path | None = None,
        failures_threshold: int | None = None,
        window_minutes: int | None = None,
    ) -> None:
        rule = _load_brute_force_rule(rules_path)

        self._failures_threshold: int = failures_threshold or int(
            rule.get("failures_threshold", _DEFAULT_FAILURES_THRESHOLD)
        )
        self._window_seconds: float = float(
            (window_minutes or int(rule.get("window_minutes", _DEFAULT_WINDOW_MINUTES))) * 60
        )
        self._severity: str = str(rule.get("severity", _DEFAULT_SEVERITY))

        # source_ip -> list of event timestamps (epoch seconds)
        self._failure_events: Dict[str, List[float]] = {}

    # ── public API ──────────────────────────────────────────────────── #

    def detect(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Analyse a security event for brute force patterns.

        Parameters
        ----------
        event : dict
            A security event dict.  Must contain ``source_ip`` and indicate
            an auth failure (via ``"auth_failure"`` in ``event_type``,
            ``"failed"`` in ``message``, or ``attack_type`` containing
            ``"auth"`` / ``"login"`` / ``"failed"``).

        Returns
        -------
        dict | None
            A ``SecurityAnomalySignal`` dict when the brute-force threshold
            is reached, otherwise ``None``.
        """
        if not self._is_auth_failure(event):
            return None

        source_ip = str(event.get("source_ip", "unknown"))
        now = time.time()

        # Prune events outside the sliding window
        timestamps = self._failure_events.setdefault(source_ip, [])
        cutoff = now - self._window_seconds
        self._failure_events[source_ip] = [t for t in timestamps if t > cutoff]

        # Record this event
        self._failure_events[source_ip].append(now)

        count = len(self._failure_events[source_ip])

        if count < self._failures_threshold:
            return None

        # Build SecurityAnomalySignal
        confidence = min(count * 2, 100.0)
        entity_id = f"brute-force-{source_ip}"

        signal: Dict[str, Any] = {
            "attack_type": "BRUTE_FORCE_ATTEMPT",
            "entity_id": entity_id,
            "severity": self._severity,
            "confidence": confidence,
            "evidence_logs": self._build_evidence(source_ip),
            "recommended_action": f"Block source IP {source_ip} immediately",
            "source_ip": source_ip,
            "timestamp": self._iso_timestamp(now),
        }

        logger.warning(
            "Brute force detected — source_ip=%s count=%d confidence=%.1f",
            source_ip,
            count,
            confidence,
        )

        return signal

    @property
    def failures_threshold(self) -> int:
        """Return the configured failure count threshold."""
        return self._failures_threshold

    @property
    def window_seconds(self) -> float:
        """Return the configured time window in seconds."""
        return self._window_seconds

    # ── helpers ─────────────────────────────────────────────────────── #

    @staticmethod
    def _is_auth_failure(event: Dict[str, Any]) -> bool:
        """Return True if the event indicates an authentication failure."""
        # Check explicit attack_type
        attack_type = str(event.get("attack_type", "")).lower()
        if "auth" in attack_type or "login" in attack_type or "failed" in attack_type:
            return True

        # Check event_type field
        event_type = str(event.get("event_type", "")).lower()
        if "auth" in event_type or "login" in event_type or "failed" in event_type:
            return True

        # Check message/description for auth failure indicators
        for field in ("message", "description", "log"):
            text = str(event.get(field, "")).lower()
            if any(kw in text for kw in ("auth_fail", "login_fail", "failed_login", "failed_password", "authentication failure")):
                return True

        return False

    def _build_evidence(self, source_ip: str) -> List[str]:
        """Build evidence log snippets from recent failure events."""
        timestamps = self._failure_events.get(source_ip, [])
        count = len(timestamps)
        return [
            f"Auth failure #{i+1} from {source_ip} at {self._iso_timestamp(t)}"
            for i, t in enumerate(timestamps[-5:])  # last 5 events
        ]

    @staticmethod
    def _iso_timestamp(epoch: float) -> str:
        """Convert epoch seconds to ISO 8601 UTC string."""
        from datetime import datetime, timezone
        return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
