"""
OmniWatch — Generative AI Layer
Component: Incident Summary Generator
Phase: 10
Purpose: Generates ops-engineer incident summaries grounded in a RootCauseObject.
         Uses GroundedLLMClient (GAP4) for all generation — never raw Ollama.
Inputs: RootCauseObject from causal engine
Outputs: GroundedAnalysis with summary targeted at operations engineers
"""

from __future__ import annotations

import json
import logging

from genai.grounded_llm_client import GroundedLLMClient
from genai.models import GroundedAnalysis, RootCauseObject

logger = logging.getLogger(__name__)

_SUMMARY_SYSTEM_PROMPT = """You are an incident summary generator for operations engineers.
Given a RootCauseObject, produce a concise technical summary covering:
1. What failed and why (root cause)
2. Which services are impacted
3. Current severity and confidence
4. Recommended immediate actions

Output ONLY valid JSON matching the GroundedAnalysis schema.
Reference ONLY entities present in the RootCauseObject."""


class IncidentSummaryGenerator:
    """Generates ops-engineer incident summaries via grounded LLM."""

    def __init__(self, client: GroundedLLMClient | None = None) -> None:
        self._client = client or GroundedLLMClient()

    async def generate(self, root_cause: RootCauseObject) -> GroundedAnalysis:
        """Generate a grounded incident summary from a RootCauseObject.

        Args:
            root_cause: The RootCauseObject to summarize.

        Returns:
            GroundedAnalysis with technical summary for ops engineers.
        """
        logger.info(
            json.dumps({
                "event": "incident_summary_generate",
                "incident_id": root_cause.incident_id,
            })
        )
        return await self._client.generate(root_cause)

    async def close(self) -> None:
        """Close the underlying LLM client."""
        await self._client.close()
