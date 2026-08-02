"""
OmniWatch — Predictive Intelligence Layer
Component: Config Drift Detector (GAP 1)
Phase: 6
Purpose: Detect unauthorized configuration changes from security events
Inputs: Security event dicts (from omniwatch.security.events Kafka topic)
Outputs: SecurityAnomalySignal for UNAUTHORIZED_CONFIG_CHANGE or None
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path
from typing import Any

try:
    import yaml  # type: ignore[import-untyped]
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────── #

_DEFAULT_RULES_PATH = Path(__file__).resolve().parent.parent / "config" / "security_rules.yaml"

# Patterns that indicate a config drift event
_CONFIG_DRIFT_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"config_file_changed", re.IGNORECASE),
    re.compile(r"CONFIG_DRIFT", re.IGNORECASE),
    re.compile(r"config.*drift", re.IGNORECASE),
    re.compile(r"unauthorized.*config.*change", re.IGNORECASE),
]

# Default severity for unauthorized config changes (per task spec)
_DEFAULT_SEVERITY = "CRITICAL"


# ─── Helpers ──────────────────────────────────────────────────────────────── #

def _load_rules(path: Path | None = None) -> dict[str, Any]:
    """Load config_drift rule from security_rules.yaml.

    Returns the ``config_drift`` section, or a minimal fallback dict when
    the file is missing or pyyaml is not installed.
    """
    fallback: dict[str, Any] = {
        "trigger": "unauthorized_config_change",
        "severity": _DEFAULT_SEVERITY,
        "description": "Unauthorized configuration change detected",
    }

    rules_path = path or _DEFAULT_RULES_PATH
    if not rules_path.exists():
        logger.warning("security_rules.yaml not found at %s — using defaults", rules_path)
        return fallback

    if yaml is None:
        logger.warning("pyyaml not installed — using default config_drift rules")
        return fallback

    try:
        with open(rules_path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        attack_types = data.get("attack_types", {})
        drift_rule = attack_types.get("config_drift", {})
        if not drift_rule:
            logger.warning("config_drift rule missing from security_rules.yaml — using defaults")
            return fallback
        return drift_rule
    except Exception:
        logger.warning("Failed to parse security_rules.yaml — using defaults", exc_info=True)
        return fallback


def _matches_config_drift(event: dict[str, Any]) -> bool:
    """Return True if the event contains a config-drift indicator."""
    # Check explicit attack_type / event_type fields
    for field in ("attack_type", "event_type", "type", "description", "message"):
        value = event.get(field)
        if isinstance(value, str):
            for pattern in _CONFIG_DRIFT_PATTERNS:
                if pattern.search(value):
                    return True

    # Check attributes sub-dict (common in OTel-normalized events)
    attrs = event.get("attributes")
    if isinstance(attrs, dict):
        for val in attrs.values():
            if isinstance(val, str):
                for pattern in _CONFIG_DRIFT_PATTERNS:
                    if pattern.search(val):
                        return True

    # Check the full event payload as a fallback (flat JSON events)
    event_type_str = event.get("event", "")
    if isinstance(event_type_str, str) and event_type_str:
        for pattern in _CONFIG_DRIFT_PATTERNS:
            if pattern.search(event_type_str):
                return True

    return False


def _is_approved(event: dict[str, Any], approved_changes: set[str] | None) -> bool:
    """Check whether the config change is in the approved-change list.

    The approved list can match on:
    - ``change_id`` field
    - ``config_file`` / ``file`` / ``path`` field
    - ``description`` field
    """
    if not approved_changes:
        return False

    for field in ("change_id", "config_file", "file", "path", "description", "message"):
        value = event.get(field)
        if isinstance(value, str) and value in approved_changes:
            return True

    # Also check attributes
    attrs = event.get("attributes")
    if isinstance(attrs, dict):
        for key in ("change_id", "config_file", "file", "path"):
            val = attrs.get(key)
            if isinstance(val, str) and val in approved_changes:
                return True

    return False


# ─── Main class ───────────────────────────────────────────────────────────── #


class ConfigDriftDetector:
    """Detects unauthorized configuration changes from security events.

    Grep security events for ``config_file_changed`` or ``CONFIG_DRIFT``
    patterns.  If the change is NOT in the approved-change list, emit a
    ``SecurityAnomalySignal`` with ``attack_type=UNAUTHORIZED_CONFIG_CHANGE``
    and ``severity=CRITICAL``.

    Parameters
    ----------
    approved_changes:
        Set of identifiers (change IDs, file paths, or descriptions) that
        are pre-approved and should NOT trigger an alert.
    rules_path:
        Optional path to ``security_rules.yaml``.  When *None* the default
        location ``predictive/config/security_rules.yaml`` is used.
    """

    def __init__(
        self,
        approved_changes: set[str] | None = None,
        rules_path: Path | None = None,
    ) -> None:
        self._approved_changes: set[str] = approved_changes or set()
        self._rule = _load_rules(rules_path)
        logger.info(
            "ConfigDriftDetector initialised — severity=%s, approved_count=%d",
            self._rule.get("severity", _DEFAULT_SEVERITY),
            len(self._approved_changes),
        )

    # -- Public API -----------------------------------------------------------

    def detect(self, event: dict[str, Any]) -> dict[str, Any] | None:
        """Analyse a security event for unauthorized config drift.

        Parameters
        ----------
        event:
            A security event dict (from ``omniwatch.security.events``).

        Returns
        -------
        dict or None
            A ``SecurityAnomalySignal`` dict when an **unauthorised** config
            change is detected, otherwise ``None``.
        """
        if not isinstance(event, dict):
            logger.debug("ConfigDriftDetector: non-dict event ignored (%s)", type(event).__name__)
            return None

        # 1. Does the event match a config-drift pattern?
        if not _matches_config_drift(event):
            return None

        # 2. Is the change pre-approved?
        if _is_approved(event, self._approved_changes):
            entity_id = event.get("entity_id", "unknown")
            logger.info(
                "ConfigDriftDetector: approved config change on %s — no alert",
                entity_id,
            )
            return None

        # 3. Build the SecurityAnomalySignal
        entity_id = event.get("entity_id", event.get("source", "unknown"))
        source_ip = event.get("source_ip", event.get("sourceIp", None))
        timestamp = event.get("timestamp", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))

        # Build evidence from available fields
        evidence_parts: list[str] = []
        for field in ("description", "message", "event_type", "attack_type", "type"):
            val = event.get(field)
            if isinstance(val, str) and val:
                evidence_parts.append(f"{field}: {val}")

        attrs = event.get("attributes")
        if isinstance(attrs, dict):
            for k, v in attrs.items():
                if isinstance(v, str) and v:
                    evidence_parts.append(f"attr.{k}: {v}")

        if not evidence_parts:
            evidence_parts.append(f"raw_event: {str(event)[:500]}")

        signal: dict[str, Any] = {
            "attack_type": "UNAUTHORIZED_CONFIG_CHANGE",
            "entity_id": entity_id,
            "severity": _DEFAULT_SEVERITY,
            "confidence": 90.0,
            "evidence_logs": evidence_parts,
            "recommended_action": (
                "Investigate unauthorized configuration change. "
                "Verify the change was not initiated by an approved deployment pipeline. "
                "Consider reverting to last known-good configuration."
            ),
            "source_ip": source_ip,
            "timestamp": timestamp,
        }

        logger.warning(
            "ConfigDriftDetector: UNAUTHORIZED config change detected — "
            "entity=%s source_ip=%s",
            entity_id,
            source_ip,
        )
        return signal

    # -- Accessors ------------------------------------------------------------

    @property
    def approved_changes(self) -> frozenset[str]:
        """Return the current approved-change set (read-only view)."""
        return frozenset(self._approved_changes)

    def add_approved(self, change_id: str) -> None:
        """Add a change identifier to the approved list."""
        self._approved_changes.add(change_id)

    def remove_approved(self, change_id: str) -> None:
        """Remove a change identifier from the approved list."""
        self._approved_changes.discard(change_id)

    @property
    def rule(self) -> dict[str, Any]:
        """Return the loaded config_drift rule from security_rules.yaml."""
        return dict(self._rule)
