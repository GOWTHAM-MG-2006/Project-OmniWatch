"""
OmniWatch — Generative AI Layer
Component: Grounded LLM Client (GAP4)
Phase: 10
Purpose: Async httpx client to Ollama /api/generate or vLLM /v1/completions.
         Strict system prompt enforcing "cite ONLY entities present in the
         supplied RootCauseObject". JSON-only output; think:false to prevent
         thinking channel corruption. Retries up to 2 on validation failure
         via output_validator. Deterministic fallback when all retries exhausted.
Inputs: RootCauseObject from causal engine
Outputs: GroundedAnalysis (validated JSON from LLM)
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any

import httpx

from genai.models import GroundedAnalysis, RootCauseObject
from genai.output_validator import validate_output
from genai.settings import Settings

logger = logging.getLogger(__name__)

# Retry constants (FM-5: exponential backoff with cap)
_LLM_RETRY_DELAY: float = 0.5
_LLM_RETRY_MULTIPLIER: float = 2.0
_LLM_RETRY_MAX_DELAY: float = 8.0
_LLM_MAX_RETRIES: int = 2

# System prompt — enforces grounded output
_SYSTEM_PROMPT = """You are OmniWatch, an AIOps root-cause analyst. You MUST ground ALL your analysis in the supplied RootCauseObject.

RULES (MANDATORY):
1. You may ONLY reference entities that appear in the RootCauseObject fields:
   root_cause_entity, entity_type, fault_path, impacted_services, evidence.
2. Do NOT invent or hallucinate entity names, service names, or database names
   that are NOT present in the input data.
3. Your output MUST be valid JSON matching this exact schema:
{
  "summary": "One-paragraph root cause summary",
  "root_cause_entity": "entity from RootCauseObject",
  "confidence": 0.0-100.0,
  "recommended_actions": ["action1", "action2"],
  "impacted_entities": ["entity1", "entity2"],
  "reasoning": "Step-by-step reasoning grounded in evidence"
}
4. All entity names in root_cause_entity, impacted_entities, and recommended_actions
   MUST be extracted directly from the RootCauseObject.
5. Do NOT add any text before or after the JSON. Output ONLY the JSON object.
6. Do NOT use markdown code fences. Output raw JSON only."""

# Deterministic fallback template (blind-spot #15)
_FALLBACK_TEMPLATE: dict[str, Any] = {
    "summary": "Root cause analysis generated via fallback template. "
               "The LLM was unable to produce a grounded analysis after "
               "multiple attempts.",
    "root_cause_entity": "",
    "confidence": 0.0,
    "recommended_actions": ["Investigate the incident manually"],
    "impacted_entities": [],
    "reasoning": "Fallback template used — LLM output could not be validated "
                 "or parsed. Manual investigation required.",
}


class GroundedLLMClient:
    """Async httpx client for Ollama /api/generate or vLLM /v1/completions
    with grounded output validation.

    Sends a system prompt + RootCauseObject to the LLM, validates the output
    against the grounding source, and retries up to 2 times on validation failure.
    Uses think:false to prevent thinking channel from corrupting JSON output.
    Enforces concurrency via asyncio.Semaphore(2).
    """

    def __init__(
        self,
        ollama_url: str | None = None,
        model: str | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> None:
        settings = Settings()
        self._backend: str = settings.llm_backend
        self._vllm_base: str = settings.vllm_base
        self.ollama_url: str = ollama_url or settings.ollama_url
        self.model: str = model or settings.llm_model
        self.max_tokens: int = max_tokens or settings.llm_max_tokens
        self.temperature: float = temperature or settings.llm_temperature
        self._client = httpx.AsyncClient(timeout=120.0)
        # FM-4: concurrency bound
        self._semaphore = asyncio.Semaphore(settings.llm_concurrency)

    async def generate(
        self, root_cause: RootCauseObject
    ) -> GroundedAnalysis:
        """Generate a grounded analysis from a RootCauseObject.

        Retries up to 2 times if validation fails (hallucinated entities).
        After all retries are exhausted, returns a deterministic fallback template.

        Args:
            root_cause: The RootCauseObject to ground the analysis in.

        Returns:
            GroundedAnalysis with validated, grounded output.
        """
        user_prompt = self._build_user_prompt(root_cause)

        for attempt in range(1, _LLM_MAX_RETRIES + 2):
            try:
                async with self._semaphore:
                    if self._backend == "vllm":
                        raw_response = await self._call_vllm(user_prompt)
                    else:
                        raw_response = await self._call_ollama(user_prompt)

                parsed = self._parse_json_response(raw_response)
                validation = validate_output(parsed, root_cause, attempt=attempt)

                if validation.valid:
                    return GroundedAnalysis(**parsed)

                logger.warning(
                    json.dumps({
                        "event": "llm_output_not_grounded",
                        "attempt": attempt,
                        "hallucinated": validation.hallucinated_entities,
                    })
                )

                # FM-5: corrected prompt with validation feedback
                if attempt <= _LLM_MAX_RETRIES:
                    feedback = (
                        f"\n\n[VALIDATION FAILED — attempt {attempt}] "
                        f"Hallucinated entities: {validation.hallucinated_entities}. "
                        f"Fix: only reference entities from the RootCauseObject."
                    )
                    user_prompt = self._build_user_prompt(root_cause) + feedback
                    delay = min(
                        _LLM_RETRY_DELAY * (_LLM_RETRY_MULTIPLIER ** (attempt - 1)),
                        _LLM_RETRY_MAX_DELAY,
                    )
                    await asyncio.sleep(delay)

            except json.JSONDecodeError as exc:
                logger.warning(
                    json.dumps({
                        "event": "llm_json_parse_error",
                        "attempt": attempt,
                        "error": str(exc),
                    })
                )
                if attempt <= _LLM_MAX_RETRIES:
                    delay = min(
                        _LLM_RETRY_DELAY * (_LLM_RETRY_MULTIPLIER ** (attempt - 1)),
                        _LLM_RETRY_MAX_DELAY,
                    )
                    await asyncio.sleep(delay)

        # All retries exhausted — deterministic fallback (blind-spot #15)
        logger.warning(
            json.dumps({
                "event": "llm_fallback_template_used",
                "attempts": _LLM_MAX_RETRIES + 1,
                "incident_id": root_cause.incident_id,
            })
        )
        fallback = dict(_FALLBACK_TEMPLATE)
        fallback["root_cause_entity"] = root_cause.root_cause_entity
        fallback["impacted_entities"] = list(root_cause.impacted_services)
        return GroundedAnalysis(**fallback)

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self._client.aclose()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_user_prompt(self, root_cause: RootCauseObject) -> str:
        """Build the user prompt containing the RootCauseObject data."""
        rc_dict = root_cause.model_dump()
        return (
            "Analyze the following RootCauseObject and provide a grounded "
            "root-cause analysis. Remember: reference ONLY entities present "
            "in this data.\n\n"
            f"RootCauseObject:\n{json.dumps(rc_dict, indent=2)}"
        )

    async def _call_ollama(self, user_prompt: str) -> str:
        """Call Ollama /api/generate and return the response text."""
        url = f"{self.ollama_url}/api/generate"
        payload: dict[str, Any] = {
            "model": self.model,
            "prompt": user_prompt,
            "system": _SYSTEM_PROMPT,
            "stream": False,
            "think": False,
            "options": {
                "num_predict": self.max_tokens,
                "temperature": self.temperature,
            },
        }

        response = await self._client.post(url, json=payload)
        response.raise_for_status()
        data = response.json()
        return data.get("response", "")

    async def _call_vllm(self, user_prompt: str) -> str:
        """Call vLLM /v1/completions and return the response text."""
        url = f"{self._vllm_base}/v1/completions"
        payload: dict[str, Any] = {
            "model": self.model,
            "prompt": f"{_SYSTEM_PROMPT}\n\n{user_prompt}",
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "stream": False,
        }

        response = await self._client.post(url, json=payload)
        response.raise_for_status()
        data = response.json()
        choices = data.get("choices", [])
        if choices:
            return choices[0].get("text", "")
        return ""

    @staticmethod
    def _parse_json_response(raw: str) -> dict[str, Any]:
        """Parse the LLM response as JSON, stripping thinking blocks and fences."""
        text = raw.strip()

        # FM-1: Strip <thinking>...</thinking> blocks (qwen3 emits despite think:false)
        text = re.sub(r"<thinking>.*?</thinking>", "", text, flags=re.DOTALL)
        text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)
        text = text.strip()

        # Strip markdown code fences if present
        if text.startswith("```"):
            lines = text.split("\n")
            if lines[-1].strip() == "```":
                lines = lines[1:-1]
            elif lines[0].strip().startswith("```"):
                lines = lines[1:]
            text = "\n".join(lines).strip()

        return json.loads(text)
