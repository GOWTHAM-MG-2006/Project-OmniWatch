"""
OmniWatch — Generative AI Layer
Component: Pipeline E2E Tests
Phase: 10
Purpose: End-to-end pipeline tests covering consumer→generator→producer→MinIO.
         Each test validates a specific pipeline stage with fully mocked dependencies.
Inputs: Fixtures from conftest.py
Outputs: Pass/fail for each pipeline test
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from genai.models import (
    GeneratedReport,
    GroundedAnalysis,
    GroundedArtifact,
    PostMortem,
    RootCauseObject,
    Runbook,
)


# ---------------------------------------------------------------------------
# Helper: Mock GroundedLLMClient that returns a fixed valid response
# ---------------------------------------------------------------------------

def _patched_client(
    grounded_output: dict[str, Any] | None = None,
) -> AsyncMock:
    """Return a fully mocked GroundedLLMClient.generate()."""
    if grounded_output is None:
        grounded_output = {
            "summary": "postgresql-database",
            "root_cause_entity": "postgresql-database",
            "confidence": 90.0,
            "recommended_actions": ["restart_service"],
            "impacted_entities": ["postgresql-database", "order-service"],
            "reasoning": "postgresql-database connection pool exhausted",
        }
    mock_client = AsyncMock()
    mock_client.generate.return_value = GroundedAnalysis(**grounded_output)
    mock_client.close = AsyncMock()
    return mock_client


# ---------------------------------------------------------------------------
# E2E-1: Incident Summary Generator
# ---------------------------------------------------------------------------

class TestPipelineIncidentSummary:
    @pytest.mark.asyncio
    async def test_generate_returns_grounded_analysis(
        self, root_cause_factory: RootCauseObject
    ) -> None:
        from genai.incident_summary import IncidentSummaryGenerator

        mock_client = _patched_client()
        generator = IncidentSummaryGenerator(client=mock_client)
        result = await generator.generate(root_cause_factory)

        assert isinstance(result, GroundedAnalysis)
        assert result.root_cause_entity == "postgresql-database"
        mock_client.generate.assert_called_once_with(root_cause_factory)
        await generator.close()

    @pytest.mark.asyncio
    async def test_generate_calls_client_generate(
        self, root_cause_factory: RootCauseObject
    ) -> None:
        from genai.incident_summary import IncidentSummaryGenerator

        mock_client = _patched_client()
        generator = IncidentSummaryGenerator(client=mock_client)
        await generator.generate(root_cause_factory)

        mock_client.generate.assert_awaited_once()
        await generator.close()


# ---------------------------------------------------------------------------
# E2E-2: Executive Report Generator
# ---------------------------------------------------------------------------

class TestPipelineExecutiveReport:
    @pytest.mark.asyncio
    async def test_generate_returns_generated_report(
        self, root_cause_factory: RootCauseObject
    ) -> None:
        from genai.executive_report import ExecutiveReportGenerator

        mock_client = _patched_client()
        generator = ExecutiveReportGenerator(client=mock_client)
        result = await generator.generate(root_cause_factory)

        assert isinstance(result, GeneratedReport)
        assert result.incident_id == root_cause_factory.incident_id
        assert result.report_type == "executive_summary"
        assert result.audience == "executives"
        assert len(result.sections) == 3
        await generator.close()

    @pytest.mark.asyncio
    async def test_generate_sections_contain_impact_info(
        self, root_cause_factory: RootCauseObject
    ) -> None:
        from genai.executive_report import ExecutiveReportGenerator

        mock_client = _patched_client()
        generator = ExecutiveReportGenerator(client=mock_client)
        result = await generator.generate(root_cause_factory)

        impact_section = [s for s in result.sections if s["title"] == "Impact"]
        assert len(impact_section) == 1
        assert "postgresql-database" in impact_section[0]["content"]
        await generator.close()


# ---------------------------------------------------------------------------
# E2E-3: Runbook Generator
# ---------------------------------------------------------------------------

class TestPipelineRunbook:
    @pytest.mark.asyncio
    async def test_generate_returns_runbook(
        self, root_cause_factory: RootCauseObject
    ) -> None:
        from genai.runbook_generator import RunbookGenerator

        mock_client = _patched_client()
        generator = RunbookGenerator(client=mock_client)
        result = await generator.generate(root_cause_factory)

        assert isinstance(result, Runbook)
        assert result.incident_id == root_cause_factory.incident_id
        assert result.artifact_type == "runbook"
        assert len(result.steps) > 0
        await generator.close()

    @pytest.mark.asyncio
    async def test_generate_with_action_result_appends_verified(
        self, root_cause_factory: RootCauseObject
    ) -> None:
        from genai.runbook_generator import RunbookGenerator

        mock_client = _patched_client()
        generator = RunbookGenerator(client=mock_client)
        action_result = {"success": True, "output": "Service restarted"}
        result = await generator.generate(root_cause_factory, action_result=action_result)

        assert any("Verified" in step for step in result.steps)
        await generator.close()

    @pytest.mark.asyncio
    async def test_severity_inferred_from_anomaly_score(self) -> None:
        from genai.runbook_generator import _infer_severity

        assert _infer_severity(0.95) == "P1"
        assert _infer_severity(0.8) == "P2"
        assert _infer_severity(0.5) == "P3"
        assert _infer_severity(0.2) == "P4"


# ---------------------------------------------------------------------------
# E2E-4: Post-Incident Analyser
# ---------------------------------------------------------------------------

class TestPipelinePostIncident:
    @pytest.mark.asyncio
    async def test_generate_returns_postmortem(
        self, root_cause_factory: RootCauseObject
    ) -> None:
        from genai.post_incident_analyser import PostIncidentAnalyser

        mock_client = _patched_client()
        generator = PostIncidentAnalyser(client=mock_client)
        result = await generator.generate(root_cause_factory)

        assert isinstance(result, PostMortem)
        assert result.incident_id == root_cause_factory.incident_id
        assert result.artifact_type == "postmortem"
        assert len(result.timeline) > 0
        assert len(result.lessons_learned) > 0
        assert len(result.action_items) > 0
        await generator.close()

    @pytest.mark.asyncio
    async def test_timeline_matches_fault_path(
        self, root_cause_factory: RootCauseObject
    ) -> None:
        from genai.post_incident_analyser import PostIncidentAnalyser

        mock_client = _patched_client()
        generator = PostIncidentAnalyser(client=mock_client)
        result = await generator.generate(root_cause_factory)

        assert result.timeline == root_cause_factory.fault_path
        await generator.close()


# ---------------------------------------------------------------------------
# E2E-5: Kafka Consumer — Incident Routing
# ---------------------------------------------------------------------------

class TestPipelineConsumer:
    def test_consumer_subscribes_to_correct_topics(self) -> None:
        from genai.genai_consumer import GenAIConsumer, _CONSUME_TOPICS

        assert "omniwatch.incidents.created" in _CONSUME_TOPICS
        assert "omniwatch.remediation.actions" in _CONSUME_TOPICS

    def test_consumer_handles_incident_message(self) -> None:
        from genai.genai_consumer import GenAIConsumer

        with (
            patch("genai.genai_consumer.Consumer") as mock_consumer_cls,
            patch("genai.grounded_llm_client.GroundedLLMClient") as mock_llm_cls,
        ):
            mock_consumer_cls.return_value = MagicMock()
            consumer = GenAIConsumer()

            mock_store = MagicMock()
            mock_producer = MagicMock()
            consumer._store = mock_store
            consumer._producer = mock_producer

            mock_llm_cls.return_value = _patched_client()

            value = json.dumps({
                "incident_id": "test-001",
                "root_cause": {
                    "incident_id": "test-001",
                    "root_cause_entity": "postgresql-database",
                    "entity_type": "DATABASE_NODE",
                    "confidence": 92.0,
                    "anomaly_score": 0.85,
                    "fault_path": ["postgresql-database"],
                    "impacted_services": ["order-service"],
                    "impacted_count": 1,
                    "evidence": {},
                    "timestamp": "2024-01-01T00:00:00Z",
                },
                "severity": "P1",
                "status": "OPEN",
            }).encode("utf-8")

            msg = MagicMock()
            msg.error.return_value = None
            msg.topic.return_value = "omniwatch.incidents.created"
            msg.value.return_value = value

            consumer._handle_message(msg)
            stats = consumer.get_stats()
            assert stats["consumed"] == 1
            assert stats["generated"] == 1
            assert mock_store.persist.call_count == 3
            assert mock_producer.produce.call_count == 3

    def test_consumer_handles_remediation_message(self) -> None:
        from genai.genai_consumer import GenAIConsumer

        with (
            patch("genai.genai_consumer.Consumer") as mock_consumer_cls,
            patch("genai.grounded_llm_client.GroundedLLMClient") as mock_llm_cls,
        ):
            mock_consumer_cls.return_value = MagicMock()
            consumer = GenAIConsumer()

            mock_store = MagicMock()
            mock_producer = MagicMock()
            consumer._store = mock_store
            consumer._producer = mock_producer

            mock_llm_cls.return_value = _patched_client()

            value = json.dumps({
                "incident_id": "test-001",
                "action_type": "restart_service",
                "success": True,
            }).encode("utf-8")

            msg = MagicMock()
            msg.error.return_value = None
            msg.topic.return_value = "omniwatch.remediation.actions"
            msg.value.return_value = value

            consumer._handle_message(msg)
            stats = consumer.get_stats()
            assert stats["consumed"] == 1
            assert stats["generated"] == 1
            assert mock_store.persist.call_count == 1
            assert mock_producer.produce.call_count == 1

    def test_consumer_increments_errors_on_bad_json(self) -> None:
        from genai.genai_consumer import GenAIConsumer

        with patch("genai.genai_consumer.Consumer") as mock_consumer_cls:
            mock_consumer_cls.return_value = MagicMock()
            consumer = GenAIConsumer()

            msg = MagicMock()
            msg.error.return_value = None
            msg.topic.return_value = "omniwatch.incidents.created"
            msg.value.return_value = b"not json"

            consumer._handle_message(msg)
            stats = consumer.get_stats()
            assert stats["errors"] == 1


# ---------------------------------------------------------------------------
# E2E-6: Kafka Producer — Artifact Publishing
# ---------------------------------------------------------------------------

class TestPipelineProducer:
    def test_producer_publishes_to_correct_topic(self) -> None:
        from genai.genai_producer import GenAIProducer

        with patch("genai.genai_producer.Producer") as mock_producer_cls:
            mock_producer_instance = MagicMock()
            mock_producer_cls.return_value = mock_producer_instance
            producer = GenAIProducer()

            artifact = GroundedArtifact(
                incident_id="test-001",
                artifact_type="runbook",
                content="Runbook content",
            )
            producer.produce(artifact)
            mock_producer_instance.produce.assert_called_once()
            call_args = mock_producer_instance.produce.call_args
            assert call_args[0][0] == "omniwatch.generated.runbooks"

    def test_producer_increments_stats(self) -> None:
        from genai.genai_producer import GenAIProducer

        with patch("genai.genai_producer.Producer") as mock_producer_cls:
            mock_producer_instance = MagicMock()
            mock_producer_cls.return_value = mock_producer_instance
            producer = GenAIProducer()

            artifact = GroundedArtifact(
                incident_id="test-001",
                artifact_type="summary",
                content="Summary content",
            )
            producer.produce(artifact)
            stats = producer.get_stats()
            assert stats["produced"] == 1
            assert stats["errors"] == 0

    def test_producer_handles_error_gracefully(self) -> None:
        from genai.genai_producer import GenAIProducer

        with patch("genai.genai_producer.Producer") as mock_producer_cls:
            mock_producer_instance = MagicMock()
            mock_producer_instance.produce.side_effect = Exception("Kafka down")
            mock_producer_cls.return_value = mock_producer_instance
            producer = GenAIProducer()

            artifact = GroundedArtifact(
                incident_id="test-001",
                artifact_type="summary",
                content="Summary",
            )
            producer.produce(artifact)
            stats = producer.get_stats()
            assert stats["errors"] == 1


# ---------------------------------------------------------------------------
# E2E-7: MinIO Store — Persist + Retrieve
# ---------------------------------------------------------------------------

class TestPipelineMinioStore:
    def test_persist_returns_bucket_key_path(self) -> None:
        from genai.minio_store import MinioStore

        with patch("genai.minio_store.Minio") as mock_minio_cls:
            mock_client = MagicMock()
            mock_client.bucket_exists.return_value = True
            mock_minio_cls.return_value = mock_client
            store = MinioStore()

            artifact = GroundedArtifact(
                incident_id="inc-001",
                artifact_type="runbook",
                content="Runbook content",
            )
            path = store.persist(artifact)
            assert path.startswith("omniwatch-runbooks/")
            assert "inc-001" in path

    def test_persist_calls_put_object(self) -> None:
        from genai.minio_store import MinioStore

        with patch("genai.minio_store.Minio") as mock_minio_cls:
            mock_client = MagicMock()
            mock_client.bucket_exists.return_value = True
            mock_minio_cls.return_value = mock_client
            store = MinioStore()

            artifact = GroundedArtifact(
                incident_id="inc-001",
                artifact_type="runbook",
                content="Runbook content",
            )
            store.persist(artifact)
            mock_client.put_object.assert_called_once()

    def test_auto_create_buckets(self) -> None:
        from genai.minio_store import MinioStore

        with patch("genai.minio_store.Minio") as mock_minio_cls:
            mock_client = MagicMock()
            mock_client.bucket_exists.return_value = False
            mock_minio_cls.return_value = mock_client
            MinioStore()

            created = {call[0][0] for call in mock_client.make_bucket.call_args_list}
            assert "omniwatch-runbooks" in created


# ---------------------------------------------------------------------------
# E2E-8: Full Pipeline — Generate → Validate → Persist
# ---------------------------------------------------------------------------

class TestFullPipeline:
    @pytest.mark.asyncio
    async def test_generate_validate_persist_flow(
        self, root_cause_factory: RootCauseObject
    ) -> None:
        """Simulate full pipeline: GroundedLLMClient → validate_output → MinioStore.persist."""
        from genai.grounded_llm_client import GroundedLLMClient
        from genai.minio_store import MinioStore
        from genai.output_validator import validate_output
        from genai.runbook_generator import RunbookGenerator

        # Mock LLM client
        mock_client = _patched_client()
        generator = RunbookGenerator(client=mock_client)

        # Generate
        runbook = await generator.generate(root_cause_factory)
        assert isinstance(runbook, Runbook)
        assert runbook.grounded is True

        # Validate the LLM output that fed into the runbook
        grounded_llm_output = {
            "root_cause_entity": "postgresql-database",
            "impacted_entities": ["postgresql-database", "order-service"],
        }
        report = validate_output(grounded_llm_output, root_cause_factory)
        assert report.valid is True

        # Persist
        with patch("genai.minio_store.Minio") as mock_minio_cls:
            mock_client_minio = MagicMock()
            mock_client_minio.bucket_exists.return_value = True
            mock_minio_cls.return_value = mock_client_minio
            store = MinioStore()
            path = store.persist(runbook)

        assert path.startswith("omniwatch-runbooks/")
        await generator.close()

    @pytest.mark.asyncio
    async def test_fallback_when_validation_fails(
        self, root_cause_factory: RootCauseObject
    ) -> None:
        """When LLM output contains hallucinated entities, fallback is used."""
        from genai.grounded_llm_client import GroundedLLMClient, _FALLBACK_TEMPLATE
        from genai.models import GroundedAnalysis

        mock_client = _patched_client({
            "summary": "ghost-service failure",
            "root_cause_entity": "ghost-service",
            "confidence": 85.0,
            "recommended_actions": ["restart ghost-service"],
            "impacted_entities": ["ghost-service"],
            "reasoning": "ghost-service timeout",
        })

        # Simulate validation failure → fallback
        from genai.output_validator import validate_output
        report = validate_output(
            {"root_cause_entity": "ghost-service", "impacted_entities": ["ghost-service"]},
            root_cause_factory,
        )
        assert report.valid is False
        assert len(report.hallucinated_entities) > 0

        # Use fallback template
        fallback = dict(_FALLBACK_TEMPLATE)
        fallback["root_cause_entity"] = root_cause_factory.root_cause_entity
        fallback["impacted_entities"] = list(root_cause_factory.impacted_services)
        result = GroundedAnalysis(**fallback)
        assert result.root_cause_entity == root_cause_factory.root_cause_entity
        assert result.confidence == 0.0
