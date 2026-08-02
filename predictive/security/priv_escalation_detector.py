"""
OmniWatch — Predictive Intelligence Layer
Component: Privilege Escalation Detector
Phase: 6
Purpose: Detect privilege escalation attempts by grepping logs for sudo, su, escalat, role_change
Inputs: Security event dict from omniwatch.security.events Kafka topic
Outputs: SecurityAnomalySignal if escalation detected, None otherwise
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ── Default configuration (overridden by security_rules.yaml) ─────────── #
_DEFAULT_PATTERNS: List[str] = ["sudo", "su", "escalat", "role_change"]
_DEFAULT_SEVERITY: str = "CRITICAL"
_DEFAULT_CONFIDENCE: float = 90.0

# Known admin entities that should NOT trigger alerts for normal operations.
_ADMIN_IDENTIFIERS: frozenset[str] = frozenset({
    "root", "admin", "sysadmin", "sre", "platform-admin",
})

# Path to security rules config
_RULES_PATH = Path(__file__).parent.parent / "config" / "security_rules.yaml"


def _load_rules() -> Dict[str, Any]:
    """Load priv_escalation rule from security_rules.yaml.

    Returns the rule dict for the priv_escalation key, or an empty dict if
    the file is missing or the key is absent.
    """
    try:
        import yaml  # optional dependency — stdlib fallback if unavailable
    except ImportError:
        return {}

    try:
        with open(_RULES_PATH, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        return data.get("attack_types", {}).get("priv_escalation", {})
    except (OSError, yaml.YAMLError):
        return {}


class PrivEscalationDetector:
    """Detect privilege escalation attempts in security event logs.

    Greps log messages for patterns indicating privilege escalation:
    ``sudo``, ``su -``, ``escalat``, ``role_change``. Only fires for
    non-admin entities. Severity and confidence are loaded from
    ``security_rules.yaml`` when available, falling back to defaults.
    """

    def __init__(
        self,
        *,
        patterns: Optional[List[str]] = None,
        severity: Optional[str] = None,
        confidence: Optional[float] = None,
        admin_identifiers: Optional[frozenset[str]] = None,
    ) -> None:
        """Build the detector, optionally overriding YAML-loaded config.

        Parameters
        ----------
        patterns : list[str] | None
            Regex patterns to match against log content. ``None`` → load
            from YAML or use defaults.
        severity : str | None
            Signal severity. ``None`` → load from YAML or ``"CRITICAL"``.
        confidence : float | None
            Signal confidence (0–100). ``None`` → load from YAML or ``90.0``.
        admin_identifiers : frozenset[str] | None
            Entity IDs treated as admin. ``None`` → built-in admin set.
        """
        rules = _load_rules()

        yaml_patterns = rules.get("patterns")
        self._patterns: List[str] = patterns or yaml_patterns or _DEFAULT_PATTERNS
        self._severity: str = severity or rules.get("severity", _DEFAULT_SEVERITY)
        yaml_confidence = rules.get("confidence")
        self._confidence: float = (
            confidence
            if confidence is not None
            else float(yaml_confidence) if yaml_confidence is not None else _DEFAULT_CONFIDENCE
        )
        self._admin_identifiers = admin_identifiers or _ADMIN_IDENTIFIERS

        # Pre-compile regex for each pattern (case-insensitive)
        self._compiled: List[re.Pattern[str]] = [
            re.compile(rf"\b{re.escape(p)}", re.IGNORECASE) for p in self._patterns
        ]

        logger.info(
            "PrivEscalationDetector initialised — patterns=%s severity=%s "
            "confidence=%.0f",
            self._patterns,
            self._severity,
            self._confidence,
        )

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def detect(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Analyse a security event for privilege escalation indicators.

        Parameters
        ----------
        event : dict
            Must contain at least ``log_message`` (str) and ``entity_id``
            (str).  Optional: ``source_ip`` (str), ``timestamp`` (str),
            ``entity_type`` (str).

        Returns
        -------
        dict | None
            A ``SecurityAnomalySignal`` dict when escalation is detected,
            or ``None`` when the event is clean or belongs to an admin entity.
        """
        log_message: str = str(event.get("log_message", ""))
        entity_id: str = str(event.get("entity_id", ""))

        if not log_message:
            return None

        # Skip admin entities — their sudo/su usage is expected behaviour
        if self._is_admin_entity(entity_id, event):
            logger.debug(
                "Skipping admin entity: %s", entity_id,
            )
            return None

        # Check for escalation pattern matches
        matched_patterns = self._match_patterns(log_message)
        if not matched_patterns:
            return None

        # Build the signal
        source_ip: Optional[str] = event.get("source_ip")
        timestamp: str = event.get("timestamp") or datetime.now(
            timezone.utc
        ).isoformat()

        signal: Dict[str, Any] = {
            "attack_type": "PRIVILEGE_ESCALATION_ATTEMPT",
            "entity_id": entity_id,
            "severity": self._severity,
            "confidence": self._confidence,
            "evidence_logs": [log_message],
            "recommended_action": (
                "Immediately investigate privilege escalation on "
                f"{entity_id}. Review sudo/su usage, check for unauthorized "
                "role changes, and revoke elevated permissions if unapproved."
            ),
            "source_ip": source_ip,
            "timestamp": timestamp,
        }

        logger.warning(
            "Privilege escalation detected — entity=%s matched=%s "
            "severity=%s confidence=%.0f",
            entity_id,
            matched_patterns,
            self._severity,
            self._confidence,
        )

        return signal

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _is_admin_entity(
        self, entity_id: str, event: Dict[str, Any],
    ) -> bool:
        """Return True if the entity is a known admin."""
        if entity_id.lower() in self._admin_identifiers:
            return True
        # Also check entity_type for admin-like roles
        entity_type = str(event.get("entity_type", "")).lower()
        return "admin" in entity_type

    def _match_patterns(self, log_message: str) -> List[str]:
        """Return list of pattern names that matched the log message."""
        matched: List[str] = []
        for pattern, compiled in zip(self._patterns, self._compiled):
            if compiled.search(log_message):
                matched.append(pattern)
        return matched
