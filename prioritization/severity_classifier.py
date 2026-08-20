"""
OmniWatch — Incident Prioritization
Component: Severity Classifier
Phase: 8
Purpose: Classify root cause objects into P1-P4 severity using rules
         loaded from classification_rules.yaml.
Inputs: RootCauseObject (dict or model) with normalized confidence (0..100)
Outputs: Severity string: "P1", "P2", "P3", or "P4"
"""

from __future__ import annotations

import logging
import os
from typing import Any

import yaml

from prioritization.models import RootCauseObject, normalize_confidence
from storage.common import StorageError, create_logger

_LOG: logging.Logger = create_logger("omniwatch.prioritization.severity_classifier")

_DEFAULT_RULES_PATH = os.path.join(
    os.path.dirname(__file__), "config", "classification_rules.yaml"
)

# P1 severity conditions: ALL must be true (AND)
# P2/P3: ANY must be true (OR, via conditions_any)
# P4: catch-all "else"


class SeverityClassifier:
    """Classifies root cause objects into P1-P4 severity tiers.

    Rules are loaded from classification_rules.yaml and support two
    condition modes:
    - ``conditions`` (list): ALL must match (AND logic)
    - ``conditions_any`` (list): ANY must match (OR logic)

    Confidence is expected in the 0..1 scale from Phase 7 and is
    normalized to 0..100 internally via ``normalize_confidence()``.
    """

    def __init__(self, rules_path: str | None = None) -> None:
        self._rules_path = rules_path or _DEFAULT_RULES_PATH
        self._rules: dict[str, Any] = self._load_rules()

    @staticmethod
    def _load_rules() -> dict[str, Any]:
        """Load classification rules from YAML config file."""
        path = _DEFAULT_RULES_PATH
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh)
            if not isinstance(data, dict) or "severity" not in data:
                raise StorageError(f"invalid classification rules in {path}")
            return data["severity"]
        except FileNotFoundError as exc:
            raise StorageError(f"classification rules file not found: {path}") from exc
        except yaml.YAMLError as exc:
            raise StorageError(f"failed to parse classification rules: {exc}") from exc

    @property
    def rules(self) -> dict[str, Any]:
        """Return a copy of the loaded severity rules."""
        return dict(self._rules)

    def classify(self, root_cause: RootCauseObject | dict[str, Any]) -> str:
        """Classify a root cause object into P1, P2, P3, or P4.

        Args:
            root_cause: RootCauseObject (Pydantic model or dict).

        Returns:
            Severity string: "P1", "P2", "P3", or "P4".
        """
        rc = root_cause if isinstance(root_cause, dict) else root_cause.model_dump()
        confidence_100 = normalize_confidence(float(rc.get("confidence", 0.0)))
        confidence_1 = float(rc.get("confidence", 0.0))
        anomaly_score = float(rc.get("anomaly_score", 0.0))
        impacted_count = int(rc.get("impacted_count", 0))
        entity_type = str(rc.get("entity_type", ""))

        _LOG.debug(
            "classify: entity=%s conf=%s conf100=%.1f anomaly=%s impacted=%d",
            rc.get("root_cause_entity"),
            confidence_1,
            confidence_100,
            anomaly_score,
            impacted_count,
        )

        # P1: ALL conditions must be true (AND)
        p1 = self._rules.get("p1", {})
        if p1 and self._match_all(
            p1.get("conditions", []),
            rc,
            confidence_100,
            anomaly_score,
            impacted_count,
            entity_type,
        ):
            return "P1"

        # P2: ANY condition must be true (OR)
        p2 = self._rules.get("p2", {})
        if p2 and self._match_any(
            p2.get("conditions_any", []),
            rc,
            confidence_100,
            anomaly_score,
            impacted_count,
            entity_type,
        ):
            return "P2"

        # P3: ANY condition must be true (OR)
        p3 = self._rules.get("p3", {})
        if p3 and self._match_any(
            p3.get("conditions_any", []),
            rc,
            confidence_100,
            anomaly_score,
            impacted_count,
            entity_type,
        ):
            return "P3"

        # P4: catch-all
        return "P4"

    def _match_all(
        self,
        conditions: list[dict[str, Any]],
        rc: dict[str, Any],
        confidence_100: float,
        anomaly_score: float,
        impacted_count: int,
        entity_type: str,
    ) -> bool:
        """ALL conditions must match (AND logic)."""
        for cond in conditions:
            if not self._match_condition(
                cond, rc, confidence_100, anomaly_score, impacted_count, entity_type
            ):
                return False
        return True

    def _match_any(
        self,
        conditions: list[dict[str, Any]],
        rc: dict[str, Any],
        confidence_100: float,
        anomaly_score: float,
        impacted_count: int,
        entity_type: str,
    ) -> bool:
        """ANY condition must match (OR logic)."""
        for cond in conditions:
            if self._match_condition(
                cond, rc, confidence_100, anomaly_score, impacted_count, entity_type
            ):
                return True
        return False

    @staticmethod
    def _match_condition(
        cond: dict[str, Any],
        rc: dict[str, Any],
        confidence_100: float,
        anomaly_score: float,
        impacted_count: int,
        entity_type: str,
    ) -> bool:
        """Evaluate a single condition against the root cause object."""
        if "entity_type_contains" in cond:
            needle = cond["entity_type_contains"]
            return needle in entity_type
        if "confidence_gte" in cond:
            return confidence_100 >= cond["confidence_gte"]
        if "anomaly_score_gte" in cond:
            threshold = cond["anomaly_score_gte"]
            # anomaly_score is 0..1; if threshold < 1, compare directly
            # if threshold >= 1, assume it's already 0..100 and normalize
            if threshold >= 1.0:
                return anomaly_score * 100.0 >= float(threshold)
            return anomaly_score >= float(threshold)
        if "impacted_count_gte" in cond:
            return impacted_count >= cond["impacted_count_gte"]
        if (
            "conditions" in cond
            and isinstance(cond["conditions"], str)
            and cond["conditions"] == "else"
        ):
            return True
        _LOG.warning("unknown condition key in rule: %s", list(cond.keys()))
        return False
