"""
OmniWatch — Generative AI Layer
Component: Output Validator Tests
Phase: 10
Purpose: Unit tests for output_validator.py — validates entity grounding logic,
         hallucination detection, and retry contract.
Inputs: None (uses fixtures from conftest.py)
Outputs: Test results via pytest
"""

from __future__ import annotations

from typing import Any


from genai.models import RootCauseObject, ValidationReport
from genai.output_validator import (
    _extract_entity_ids,
    _extract_referenced_entities,
    validate_output,
)


class TestExtractEntityIds:
    """Tests for _extract_entity_ids — extracting all entities from RootCauseObject."""

    def test_extracts_root_cause_entity(self, root_cause_factory: RootCauseObject) -> None:
        """Root cause entity is included."""
        entities = _extract_entity_ids(root_cause_factory)
        assert "postgresql-database" in entities

    def test_extracts_entity_type(self, root_cause_factory: RootCauseObject) -> None:
        """Entity type is included."""
        entities = _extract_entity_ids(root_cause_factory)
        assert "database_node" in entities

    def test_extracts_fault_path(self, root_cause_factory: RootCauseObject) -> None:
        """All fault path nodes are included."""
        entities = _extract_entity_ids(root_cause_factory)
        assert "postgresql-database" in entities
        assert "order-service" in entities
        assert "api-gateway" in entities

    def test_extracts_impacted_services(self, root_cause_factory: RootCauseObject) -> None:
        """Impacted services are included."""
        entities = _extract_entity_ids(root_cause_factory)
        assert "order-service" in entities
        assert "api-gateway" in entities

    def test_extracts_evidence_keys(self, root_cause_factory: RootCauseObject) -> None:
        """Evidence keys are included."""
        entities = _extract_entity_ids(root_cause_factory)
        assert "log_snippets" in entities
        assert "metrics" in entities
        assert "anomaly_timeline" in entities

    def test_extracts_evidence_string_values(self, root_cause_factory: RootCauseObject) -> None:
        """Evidence string values are included."""
        entities = _extract_entity_ids(root_cause_factory)
        assert "error: connection refused" in entities

    def test_case_insensitive(self, root_cause_factory: RootCauseObject) -> None:
        """All extracted entities are lowercase."""
        entities = _extract_entity_ids(root_cause_factory)
        for entity in entities:
            assert entity == entity.lower()


class TestExtractReferencedEntities:
    """Tests for _extract_referenced_entities — extracting entities from LLM output.

    The function walks JSON values and treats each string as a whole entity
    candidate (not tokenizing). So a summary string becomes one entity, while
    list items like "order-service" are extracted individually.
    """

    def test_extracts_from_list_fields(self) -> None:
        """Entities in list fields are extracted as whole strings."""
        output = {"impacted_entities": ["order-service", "api-gateway"]}
        entities = _extract_referenced_entities(output)
        assert "order-service" in entities
        assert "api-gateway" in entities

    def test_extracts_root_cause_entity_field(self) -> None:
        """root_cause_entity string field is extracted as a whole entity."""
        output = {"root_cause_entity": "postgresql-database"}
        entities = _extract_referenced_entities(output)
        assert "postgresql-database" in entities

    def test_extracts_from_nested_dict_values(self) -> None:
        """Dict values are walked recursively; keys are NOT collected."""
        output = {"evidence": {"postgresql-database": "redis-cache"}}
        entities = _extract_referenced_entities(output)
        assert "redis-cache" in entities
        assert "postgresql-database" not in entities

    def test_rejects_multi_word_summary_strings(self) -> None:
        """Multi-word summary strings with filler words are rejected."""
        output = {"summary": "The postgresql-database failed."}
        entities = _extract_referenced_entities(output)
        # Multi-word string with filler word "the" → rejected by _looks_like_entity
        assert "the postgresql-database failed." not in entities

    def test_filters_filler_words(self) -> None:
        """Common filler words are not treated as entities."""
        output = {"summary": "The the is a service"}
        entities = _extract_referenced_entities(output)
        assert "the" not in entities
        assert "is" not in entities

    def test_skips_long_strings(self) -> None:
        """Very long strings (>100 chars) are not treated as entities."""
        long_str = "a" * 101
        output = {"summary": long_str}
        entities = _extract_referenced_entities(output)
        assert long_str.lower() not in entities

    def test_rejects_compound_action_strings(self) -> None:
        """Compound action strings with filler prefixes are rejected."""
        output = {"recommended_actions": ["restart_service postgresql-database"]}
        entities = _extract_referenced_entities(output)
        assert "restart_service postgresql-database" not in entities


class TestValidateOutput:
    """Tests for validate_output — the main validation function."""

    def test_valid_grounded_output(
        self,
        root_cause_factory: RootCauseObject,
        grounded_output: dict[str, Any],
    ) -> None:
        """Valid grounded output passes validation."""
        report = validate_output(grounded_output, root_cause_factory, attempt=1)
        assert report.valid is True
        assert len(report.hallucinated_entities) == 0
        assert report.attempt == 1

    def test_hallucinated_entity_detected(
        self,
        root_cause_factory: RootCauseObject,
        hallucinated_output: dict[str, Any],
    ) -> None:
        """Hallucinated entity is detected and flagged."""
        report = validate_output(hallucinated_output, root_cause_factory, attempt=1)
        assert report.valid is False
        assert "redis-cache" in report.hallucinated_entities

    def test_grounding_entities_populated(
        self,
        root_cause_factory: RootCauseObject,
        grounded_output: dict[str, Any],
    ) -> None:
        """Grounded entities list is populated correctly."""
        report = validate_output(grounded_output, root_cause_factory, attempt=1)
        assert "postgresql-database" in report.grounded_entities

    def test_attempt_number_recorded(
        self,
        root_cause_factory: RootCauseObject,
        grounded_output: dict[str, Any],
    ) -> None:
        """Attempt number is recorded in the report."""
        report = validate_output(grounded_output, root_cause_factory, attempt=2)
        assert report.attempt == 2

    def test_empty_output_is_valid(
        self,
        root_cause_factory: RootCauseObject,
    ) -> None:
        """Empty output (no entity references) is valid."""
        output = {"summary": "", "reasoning": ""}
        report = validate_output(output, root_cause_factory, attempt=1)
        assert report.valid is True

    def test_multiple_hallucinated_entities(
        self,
        root_cause_factory: RootCauseObject,
    ) -> None:
        """Multiple hallucinated entities are all detected."""
        output = {
            "summary": "redis-cache and memcached caused the issue.",
            "root_cause_entity": "redis-cache",
            "impacted_entities": ["memcached", "redis-cache"],
        }
        report = validate_output(output, root_cause_factory, attempt=1)
        assert report.valid is False
        assert "redis-cache" in report.hallucinated_entities
        assert "memcached" in report.hallucinated_entities


class TestValidationReport:
    """Tests for ValidationReport model."""

    def test_valid_report(self) -> None:
        """Valid report has correct fields."""
        report = ValidationReport(
            valid=True,
            hallucinated_entities=[],
            grounded_entities=["entity-a"],
            attempt=1,
        )
        assert report.valid is True
        assert report.hallucinated_entities == []
        assert report.grounded_entities == ["entity-a"]

    def test_invalid_report(self) -> None:
        """Invalid report has hallucinated entities."""
        report = ValidationReport(
            valid=False,
            hallucinated_entities=["fake-entity"],
            grounded_entities=["real-entity"],
            attempt=2,
        )
        assert report.valid is False
        assert "fake-entity" in report.hallucinated_entities
