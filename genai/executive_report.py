"""
OmniWatch — Generative AI Layer
Component: Executive Report Generator
Phase: 10
Purpose: Generates non-technical executive reports grounded in a RootCauseObject.
         Uses GroundedLLMClient (GAP4) for all generation — never raw Ollama.
Inputs: RootCauseObject from causal engine
Outputs: GeneratedReport with executive-friendly summary
"""

from __future__ import annotations

import json
import logging

from genai.grounded_llm_client import GroundedLLMClient
from genai.models import GeneratedReport, RootCauseObject

logger = logging.getLogger(__name__)


class ExecutiveReportGenerator:
    """Generates non-technical executive reports via grounded LLM."""

    def __init__(self, client: GroundedLLMClient | None = None) -> None:
        self._client = client or GroundedLLMClient()

    async def generate(self, root_cause: RootCauseObject) -> GeneratedReport:
        """Generate a grounded executive report from a RootCauseObject.

        Args:
            root_cause: The RootCauseObject to report on.

        Returns:
            GeneratedReport with executive-friendly content.
        """
        logger.info(
            json.dumps({
                "event": "executive_report_generate",
                "incident_id": root_cause.incident_id,
            })
        )
        grounded = await self._client.generate(root_cause)
        return GeneratedReport(
            incident_id=root_cause.incident_id,
            content=grounded.summary,
            report_type="executive_summary",
            audience="executives",
            sections=[
                {"title": "Root Cause", "content": grounded.reasoning},
                {"title": "Impact", "content": ", ".join(grounded.impacted_entities)},
                {"title": "Recommended Actions", "content": "; ".join(grounded.recommended_actions)},
            ],
            grounded=grounded.confidence > 0,
        )

    async def close(self) -> None:
        """Close the underlying LLM client."""
        await self._client.close()
