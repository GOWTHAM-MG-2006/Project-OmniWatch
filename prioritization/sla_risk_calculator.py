"""
OmniWatch — Incident Prioritization
Component: SLA Risk Calculator
Phase: 8
Purpose: Determine SLA breach risk (HIGH/MEDIUM/LOW) for an incident based
         on severity and business impact score.
Inputs: severity ("P1".."P4"), business_impact_score (0.0..100.0)
Outputs: SLA risk string: "HIGH", "MEDIUM", or "LOW"
"""

from __future__ import annotations

import logging

from storage.common import create_logger

_LOG: logging.Logger = create_logger("omniwatch.prioritization.sla_risk_calculator")

# SLA breach risk table per classification_rules.yaml
# P1 → HIGH, P2 → MEDIUM, P3/P4 → LOW
_SEVERITY_RISK: dict[str, str] = {
    "P1": "HIGH",
    "P2": "MEDIUM",
    "P3": "LOW",
    "P4": "LOW",
}

# When business impact is very high, elevate risk regardless of severity
_HIGH_IMPACT_THRESHOLD: float = 80.0
_MEDIUM_IMPACT_THRESHOLD: float = 50.0


class SlaRiskCalculator:
    """Calculates SLA breach risk from severity and business impact score."""

    def __init__(self) -> None:
        self._severity_risk = _SEVERITY_RISK

    @property
    def severity_risk(self) -> dict[str, str]:
        return dict(self._severity_risk)

    def calculate(self, severity: str, business_impact_score: float) -> str:
        """Return SLA breach risk: "HIGH", "MEDIUM", or "LOW".

        Base risk comes from severity (P1→HIGH, P2→MEDIUM, P3/P4→LOW).
        However, high business impact (>= 80) elevates to HIGH, and
        medium impact (>= 50) elevates to MEDIUM — this ensures the
        SLA clock is correctly set for high-impact but lower-severity
        incidents that still threaten revenue.
        """
        base_risk = self._severity_risk.get(severity, "LOW")

        if business_impact_score >= _HIGH_IMPACT_THRESHOLD:
            risk = "HIGH"
        elif business_impact_score >= _MEDIUM_IMPACT_THRESHOLD:
            risk = "MEDIUM" if base_risk != "HIGH" else base_risk
        else:
            # Keep the severity-derived base risk
            risk = base_risk

        _LOG.debug(
            "sla_risk: severity=%s impact=%.1f base=%s -> %s",
            severity,
            business_impact_score,
            base_risk,
            risk,
        )
        return risk
