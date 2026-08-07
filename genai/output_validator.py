"""
OmniWatch — Generative AI Layer
Component: Output Validator (GAP4)
Phase: 10
Purpose: Post-generation entity validation — ensures every entity id/name
         referenced in the LLM-generated JSON exists in the input RootCauseObject.
         Returns pass/fail + list of hallucinated entities. Uses fuzzy/prefix
         matching (FM-2) to handle LLM variations like "postgres" ↔ "postgresql-database".
Inputs: LLM-generated JSON string, RootCauseObject (the grounding source)
Outputs: ValidationReport (valid, hallucinated_entities, grounded_entities, attempt)
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from genai.models import RootCauseObject, ValidationReport

logger = logging.getLogger(__name__)

MAX_VALIDATION_RETRIES: int = 2


def _extract_entity_ids(rc: RootCauseObject) -> set[str]:
    """Extract ALL entity identifiers from a RootCauseObject.

    Returns a set of lowercase strings that represent every entity
    the LLM is allowed to reference.  Includes:
    - root_cause_entity
    - entity_type
    - fault_path entries
    - impacted_services entries
    - evidence keys and string values
    """
    entities: set[str] = set()

    # Core fields
    entities.add(rc.root_cause_entity.lower())
    entities.add(rc.entity_type.lower())

    # Fault path
    for node in rc.fault_path:
        entities.add(node.lower())

    # Impacted services
    for svc in rc.impacted_services:
        entities.add(svc.lower())

    # Evidence — extract string values that look like entity names
    for key, value in rc.evidence.items():
        entities.add(key.lower())
        if isinstance(value, str):
            entities.add(value.lower())
        elif isinstance(value, dict):
            for k, v in value.items():
                entities.add(k.lower())
                if isinstance(v, str):
                    entities.add(v.lower())
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, str):
                    entities.add(item.lower())

    return entities


def _normalize_entity(text: str) -> str:
    """Normalize entity name for fuzzy matching — lowercase, strip hyphens/underscores."""
    return text.lower().replace("-", "").replace("_", "").strip()


def _fuzzy_match(entity: str, grounding_set: set[str]) -> bool:
    """FM-2: Check if entity matches any grounding entity via fuzzy/prefix matching.

    Matches:
    1. Exact match
    2. Prefix containment (entity is prefix of a grounding entity)
    3. Normalized match (after stripping hyphens/underscores)
    """
    normalized = _normalize_entity(entity)

    # Exact match
    if entity in grounding_set:
        return True

    # Prefix containment — "postgres" matches "postgresql-database"
    for ge in grounding_set:
        if ge.startswith(entity) or entity.startswith(ge):
            return True
        # Normalized prefix match
        ge_norm = _normalize_entity(ge)
        if ge_norm.startswith(normalized) or normalized.startswith(ge_norm):
            return True

    return False


def _extract_referenced_entities(llm_output: dict[str, Any]) -> set[str]:
    """Extract all entity-like strings from the LLM JSON output.

    Recursively walks the JSON structure and collects string values
    that look like entity references (non-empty, not pure punctuation,
    not common filler words).
    """
    entities: set[str] = set()
    _walk_json_for_entities(llm_output, entities)
    return entities


def _walk_json_for_entities(obj: Any, collected: set[str]) -> None:
    """Recursively walk JSON structure collecting entity-like strings."""
    if isinstance(obj, str):
        stripped = obj.strip()
        if stripped and _looks_like_entity(stripped):
            collected.add(stripped.lower())
    elif isinstance(obj, dict):
        for value in obj.values():
            _walk_json_for_entities(value, collected)
    elif isinstance(obj, list):
        for item in obj:
            _walk_json_for_entities(item, collected)


# Words that are clearly not entity references
_FILLER_WORDS: set[str] = {
    "the", "a", "an", "is", "are", "was", "were", "be", "been",
    "being", "have", "has", "had", "do", "does", "did", "will",
    "would", "could", "should", "may", "might", "can", "shall",
    "to", "of", "in", "for", "on", "with", "at", "by", "from",
    "as", "into", "through", "during", "before", "after", "above",
    "below", "between", "out", "off", "over", "under", "again",
    "further", "then", "once", "here", "there", "when", "where",
    "why", "how", "all", "each", "every", "both", "few", "more",
    "most", "other", "some", "such", "no", "nor", "not", "only",
    "own", "same", "so", "than", "too", "very", "just", "because",
    "but", "and", "or", "if", "while", "that", "this", "these",
    "those", "it", "its", "he", "she", "they", "we", "you", "i",
    "me", "my", "your", "his", "her", "our", "their", "what",
    "which", "who", "whom", "up", "down", "about", "against",
    "summary", "reasoning", "action", "actions", "entity",
    "service", "services", "database", "infrastructure",
    "root", "cause", "impact", "impacted", "recommended",
    "analysis", "confidence", "evidence", "metric", "metrics",
    "high", "medium", "low", "critical", "p1", "p2", "p3", "p4",
    "true", "false", "none", "null", "error", "warning", "info",
    "level", "type", "name", "id", "value", "status", "result",
    "output", "input", "data", "log", "logs", "trace", "traces",
    "incident", "anomaly", "anomalies", "score", "risk",
    "performance", "security", "availability", "latency",
    "timeout", "connection", "refused", "failed", "success",
    "restart", "rollback", "scale", "deploy", "deployment",
    "restart_service", "rollback_deployment", "scale_deployment",
    "clear_cache", "kill_pod", "block_ip", "rotate_credentials",
    "step", "steps", "based", "system", "check", "ensure",
    "monitor", "review", "verify", "investigate", "examine",
    "assess", "evaluate", "determine", "identify", "address",
    "resolve", "fix", "repair", "restore", "recover",
}


def _looks_like_entity(text: str) -> bool:
    """Heuristic: does this string look like an entity reference?

    Returns True if the string is short enough, contains alphanumeric
    chars, is not a common filler word, and does not look like a
    natural-language sentence (multi-word strings containing filler words).
    """
    lower = text.lower().strip()
    if not lower:
        return False
    if lower in _FILLER_WORDS:
        return False
    if not re.search(r"[a-zA-Z0-9]", lower):
        return False
    if len(lower) > 100:
        return False
    words = lower.split()
    if len(words) > 1:
        filler_count = sum(1 for w in words if w in _FILLER_WORDS)
        if filler_count > 0:
            return False
    return True


def validate_output(
    llm_output: dict[str, Any],
    root_cause: RootCauseObject,
    attempt: int = 1,
) -> ValidationReport:
    """Validate that all entities referenced in the LLM output are grounded
    in the input RootCauseObject.

    Args:
        llm_output: The parsed JSON output from the LLM.
        root_cause: The RootCauseObject used as grounding context.
        attempt: Which generation attempt this validation covers (1-based).

    Returns:
        ValidationReport with valid=True if all entities are grounded,
        or valid=False with the list of hallucinated entities.
    """
    grounding_entities = _extract_entity_ids(root_cause)
    referenced_entities = _extract_referenced_entities(llm_output)

    hallucinated: list[str] = []
    grounded: list[str] = []

    for entity in sorted(referenced_entities):
        if _fuzzy_match(entity, grounding_entities):
            grounded.append(entity)
        else:
            hallucinated.append(entity)

    valid = len(hallucinated) == 0

    if not valid:
        logger.warning(
            json.dumps({
                "event": "validation_failed",
                "attempt": attempt,
                "hallucinated_count": len(hallucinated),
                "hallucinated_entities": hallucinated,
            })
        )
    else:
        logger.info(
            json.dumps({
                "event": "validation_passed",
                "attempt": attempt,
                "grounded_count": len(grounded),
            })
        )

    return ValidationReport(
        valid=valid,
        hallucinated_entities=hallucinated,
        grounded_entities=grounded,
        attempt=attempt,
    )
