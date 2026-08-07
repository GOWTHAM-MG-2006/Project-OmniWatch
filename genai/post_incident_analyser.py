"""
OmniWatch — Generative AI Layer
Component: Post-Incident Analyser
Phase: 10
Purpose: Generates post-mortem / post-incident analysis reports grounded in a
         RootCauseObject. Uses GroundedLLMClient (GAP4) for all generation.
Inputs: RootCauseObject from causal engine
Outputs: PostMortem with timeline, root cause, lessons learned, action items
"""

from __future__ import annotations

import json
import logging

from genai.grounded_llm_client import GroundedLLMClient
from genai.models import PostMortem, RootCauseObject

logger = logging.getLogger(__name__)


class PostIncidentAnalyser:
    """Generates post-mortem reports via grounded LLM."""

    def __init__(self, client: GroundedLLMClient | None = None) -> None:
        self._client = client or GroundedLLMClient()

    async def generate(self, root_cause: RootCauseObject) -> PostMortem:
        """Generate a grounded post-mortem from a RootCauseObject.

        Args:
            root_cause: The RootCauseObject to analyze.

        Returns:
            PostMortem with timeline, root cause, lessons, action items.
        """
        logger.info(
            json.dumps({
                "event": "postmortem_generate",
                "incident_id": root_cause.incident_id,
            })
        )
        grounded = await self._client.generate(root_cause)
        return PostMortem(
            incident_id=root_cause.incident_id,
            content=grounded.summary,
            timeline=root_cause.fault_path,
            root_cause_summary=grounded.reasoning,
            lessons_learned=[
                f"Root cause: {root_cause.root_cause_entity}",
                f"Confidence: {grounded.confidence}%",
            ],
            action_items=list(grounded.recommended_actions),
            grounded=grounded.confidence > 0,
        )

    async def close(self) -> None:
        """Close the underlying LLM client."""
        await self._client.close()
