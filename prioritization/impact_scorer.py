"""
OmniWatch — Incident Prioritization
Component: Impact Scorer
Phase: 8
Purpose: Compute business impact score (0..100) for an incident.
         Formula: anomaly_score(0-40) + impacted_count*5(max25) +
                  confidence(0-15) + severity_bonus + fault_depth +
                  evidence_richness, all clamped to [0, 100].
Inputs: RootCauseObject (dict or model), severity string ("P1".."P4")
Outputs: float in [0.0, 100.0]
"""

from __future__ import annotations

import logging
from typing import Any

from prioritization.models import RootCauseObject, normalize_confidence
from storage.common import create_logger

_LOG: logging.Logger = create_logger("omniwatch.prioritization.impact_scorer")

# Severity bonus table (additive to the base score)
_SEVERITY_BONUS: dict[str, float] = {
    "P1": 20.0,
    "P2": 15.0,
    "P3": 10.0,
    "P4": 5.0,
}

# Maximum impacted-count contribution before clamping
_MAX_IMPACTED_COUNT_SCORE: float = 25.0
_IMPACTED_SCORE_PER_SERVICE: float = 5.0


class ImpactScorer:
    """Computes a 0..100 business impact score for a root cause incident."""

    def __init__(self) -> None:
        self._severity_bonus = _SEVERITY_BONUS

    @property
    def severity_bonus(self) -> dict[str, float]:
        return dict(self._severity_bonus)

    def score(self, root_cause: RootCauseObject | dict[str, Any], severity: str) -> float:
        """Compute business impact score in [0.0, 100.0].

        Components (per phase 8 build plan):
        - anomaly_score: 0..1 → scaled to 0..40
        - impacted_count: each service contributes 5, max 25
        - confidence: normalized 0..100 → scaled to 0..15
        - severity_bonus: P1=20, P2=15, P3=10, P4=5
        - fault_depth: number of entities in fault_path × 2, max 10
        - evidence_richness: log_snippets count × 1, max 5

        All components are summed and clamped to [0.0, 100.0].
        """
        rc = root_cause if isinstance(root_cause, dict) else root_cause.model_dump()

        anomaly_score = float(rc.get("anomaly_score", 0.0))
        anomaly_component = min(40.0, max(0.0, anomaly_score * 40.0))

        impacted_count = int(rc.get("impacted_count", 0))
        impacted_component = min(
            _MAX_IMPACTED_COUNT_SCORE, impacted_count * _IMPACTED_SCORE_PER_SERVICE
        )

        confidence_normalized = normalize_confidence(float(rc.get("confidence", 0.0)))
        confidence_component = min(15.0, max(0.0, confidence_normalized / 100.0 * 15.0))

        severity_component = self._severity_bonus.get(severity, 0.0)

        fault_path = rc.get("fault_path", [])
        if isinstance(fault_path, list):
            fault_depth_component = min(10.0, len(fault_path) * 2.0)
        else:
            fault_depth_component = 0.0

        evidence = rc.get("evidence", {})
        log_snippets = evidence.get("log_snippets", []) if isinstance(evidence, dict) else []
        if isinstance(log_snippets, list):
            evidence_component = min(5.0, len(log_snippets) * 1.0)
        else:
            evidence_component = 0.0

        total = (
            anomaly_component
            + impacted_component
            + confidence_component
            + severity_component
            + fault_depth_component
            + evidence_component
        )

        result = max(0.0, min(100.0, total))
        _LOG.debug(
            "impact_score: anomaly=%.1f impacted=%.1f conf=%.1f sev=%.1f depth=%.1f ev=%.1f -> %.1f",
            anomaly_component,
            impacted_component,
            confidence_component,
            severity_component,
            fault_depth_component,
            evidence_component,
            result,
        )
        return result
