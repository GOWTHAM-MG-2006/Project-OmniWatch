"""
OmniWatch — Predictive Intelligence Layer
Component: Data Exfiltration Detector
Phase: 6
Purpose: Detect data exfiltration via outbound traffic spikes (>N× rolling average)
Inputs: Security event dicts with outbound_bytes metric
Outputs: SecurityAnomalySignal dict or None
"""

from __future__ import annotations

import logging
import os
import threading
import yaml
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults (overridden by security_rules.yaml when available)
# ---------------------------------------------------------------------------
DEFAULT_OUTBOUND_RATIO: float = 3.0
DEFAULT_SEVERITY: str = "HIGH"
DEFAULT_CONFIDENCE: float = 85.0
DEFAULT_WINDOW_SECONDS: int = 60

_RULES_PATH = Path(__file__).resolve().parent.parent / "config" / "security_rules.yaml"


def _load_rules(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load data_exfil rule from security_rules.yaml.

    Returns the ``data_exfil`` subtree, or an empty dict on any failure so the
    caller can fall back to compiled defaults.
    """
    path = Path(config_path) if config_path else _RULES_PATH
    try:
        with open(path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        return data.get("attack_types", {}).get("data_exfil", {})
    except Exception:  # noqa: BLE001 — graceful degradation
        logger.warning("Could not load security rules from %s — using defaults", path)
        return {}


class DataExfilDetector:
    """Detect data exfiltration by monitoring outbound traffic spikes.

    Maintains a per-entity rolling window of ``outbound_bytes`` values.  When
    a new event's ``outbound_bytes`` exceeds the rolling average by the
    configured ratio (default 3×), a ``SecurityAnomalySignal`` is raised.

    Parameters
    ----------
    config_path : str | None
        Path to ``security_rules.yaml``.  ``None`` → use the default path.
    outbound_ratio : float | None
        Override the ratio threshold.  ``None`` → read from YAML or use default.
    window_seconds : int
        Rolling window length in seconds (default 60).
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        outbound_ratio: Optional[float] = None,
        window_seconds: int = DEFAULT_WINDOW_SECONDS,
    ) -> None:
        rules = _load_rules(config_path)
        self._outbound_ratio: float = (
            outbound_ratio
            if outbound_ratio is not None
            else float(rules.get("outbound_ratio", DEFAULT_OUTBOUND_RATIO))
        )
        # Task spec overrides YAML: severity=HIGH, confidence=85
        self._severity: str = DEFAULT_SEVERITY
        self._window_seconds: int = window_seconds

        # Per-entity rolling window: deque of (timestamp_epoch, outbound_bytes)
        self._windows: Dict[str, Deque[Tuple[float, float]]] = defaultdict(
            lambda: deque()
        )
        self._lock = threading.Lock()

        logger.info(
            "DataExfilDetector initialised — ratio=%.1f window=%ds severity=%s",
            self._outbound_ratio,
            self._window_seconds,
            self._severity,
        )

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def detect(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Analyse an event for data exfiltration.

        Parameters
        ----------
        event : dict
            Must contain ``entity_id`` (str), ``outbound_bytes`` (numeric),
            and ``timestamp`` (ISO-8601 string).  Optional: ``source_ip``.

        Returns
        -------
        dict | None
            A ``SecurityAnomalySignal`` dict when an exfiltration spike is
            detected, ``None`` otherwise.
        """
        entity_id = event.get("entity_id")
        outbound_bytes_raw = event.get("outbound_bytes")
        timestamp_str = event.get("timestamp")

        if entity_id is None or outbound_bytes_raw is None or timestamp_str is None:
            logger.debug("DataExfilDetector: skipping event with missing fields")
            return None

        try:
            outbound_bytes = float(outbound_bytes_raw)
        except (TypeError, ValueError):
            logger.debug("DataExfilDetector: non-numeric outbound_bytes — skipping")
            return None

        try:
            ts = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            logger.debug("DataExfilDetector: invalid timestamp — skipping")
            return None

        ts_epoch = ts.timestamp()

        # Evict stale entries and compute rolling average
        with self._lock:
            window = self._windows[entity_id]
            self._evict(window, ts_epoch)
            rolling_avg = self._rolling_average(window)

            # Record current observation *after* avg computation so the current
            # value is not included in its own baseline.
            window.append((ts_epoch, outbound_bytes))

        # First observation: no baseline to compare against
        if rolling_avg is None or rolling_avg <= 0:
            logger.debug(
                "DataExfilDetector: insufficient baseline for entity=%s", entity_id
            )
            return None

        ratio = outbound_bytes / rolling_avg
        if ratio <= self._outbound_ratio:
            return None

        # ── Exfiltration detected ────────────────────────────────────── #
        source_ip = event.get("source_ip")
        evidence: List[str] = [
            f"outbound_bytes={outbound_bytes:.0f}",
            f"rolling_avg={rolling_avg:.2f}",
            f"ratio={ratio:.2f}x (threshold={self._outbound_ratio:.1f}x)",
            f"window_seconds={self._window_seconds}",
        ]

        signal: Dict[str, Any] = {
            "attack_type": "DATA_EXFILTRATION",
            "entity_id": entity_id,
            "severity": self._severity,
            "confidence": DEFAULT_CONFIDENCE,
            "evidence_logs": evidence,
            "recommended_action": (
                "Investigate outbound network traffic for entity "
                f"{entity_id}. Review firewall logs and consider "
                "restricting outbound connections."
            ),
            "source_ip": source_ip,
            "timestamp": ts.isoformat(),
        }

        logger.warning(
            "DATA_EXFILTRATION detected — entity=%s ratio=%.2f avg=%.2f",
            entity_id,
            ratio,
            rolling_avg,
        )
        return signal

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _evict(self, window: Deque[Tuple[float, float]], now_epoch: float) -> None:
        """Remove entries older than the rolling window from *window*."""
        cutoff = now_epoch - self._window_seconds
        while window and window[0][0] < cutoff:
            window.popleft()

    def _rolling_average(self, window: Deque[Tuple[float, float]]) -> Optional[float]:
        """Return the mean of values in *window*, or ``None`` if empty."""
        if not window:
            return None
        total = sum(v for _, v in window)
        return total / len(window)

    def reset(self) -> None:
        """Clear all per-entity rolling windows (useful in tests)."""
        with self._lock:
            self._windows.clear()
