"""
OmniWatch — Generative AI Layer
Component: Runbook Generator
Phase: 10
Purpose: Generates step-by-step remediation runbooks grounded in a RootCauseObject
         and ActionResult. Uses GroundedLLMClient (GAP4) for all generation.
Inputs: RootCauseObject from causal engine, ActionResult from orchestration
Outputs: Runbook with ordered remediation steps
"""

from __future__ import annotations

import json
import logging
from typing import Any

from genai.grounded_llm_client import GroundedLLMClient
from genai.models import RootCauseObject, Runbook

logger = logging.getLogger(__name__)


class RunbookGenerator:
    """Generates step-by-step remediation runbooks via grounded LLM."""

    def __init__(self, client: GroundedLLMClient | None = None) -> None:
        self._client = client or GroundedLLMClient()

    async def generate(
        self,
        root_cause: RootCauseObject,
        action_result: dict[str, Any] | None = None,
    ) -> Runbook:
        """Generate a grounded runbook from a RootCauseObject.

        Args:
            root_cause: The RootCauseObject to generate a runbook for.
            action_result: Optional ActionResult from orchestration.

        Returns:
            Runbook with ordered remediation steps.
        """
        logger.info(
            json.dumps({
                "event": "runbook_generate",
                "incident_id": root_cause.incident_id,
            })
        )
        grounded = await self._client.generate(root_cause)
        steps = list(grounded.recommended_actions)
        if action_result and action_result.get("success"):
            steps.append(f"Verified: {action_result.get('output', 'Action completed')}")

        return Runbook(
            incident_id=root_cause.incident_id,
            content=grounded.summary,
            steps=steps,
            severity=_infer_severity(root_cause.anomaly_score),
            grounded=grounded.confidence > 0,
        )

    async def close(self) -> None:
        """Close the underlying LLM client."""
        await self._client.close()


def _infer_severity(anomaly_score: float) -> str:
    """Infer severity from anomaly score (0..1)."""
    if anomaly_score >= 0.9:
        return "P1"
    if anomaly_score >= 0.7:
        return "P2"
    if anomaly_score >= 0.4:
        return "P3"
    return "P4"
