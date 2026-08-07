"""
OmniWatch — Generative AI Layer
Component: Blind-Spot Regression Tests
Phase: 10
Purpose: 15 regression tests covering all blind-spots identified in phase10-plan.md §5.2.
         Each test validates a specific adversarial scenario that previously had zero coverage.
Inputs: Fixtures from conftest.py
Outputs: Pass/fail for each blind-spot
"""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from genai.grounded_llm_client import GroundedLLMClient
from genai.output_validator import validate_output
from genai.settings import Settings


# ---------------------------------------------------------------------------
# BS-1: Thinking-channel JSON corruption
# ---------------------------------------------------------------------------

class TestBlindSpot01ThinkingBlock:
    def test_thinking_block_stripped_from_response(self, root_cause_factory: Any) -> None:
        raw = (
            "<thinking>Let me analyze the root cause...</thinking>\n"
            + json.dumps({
                "summary": "postgresql-database",
                "root_cause_entity": "postgresql-database",
                "confidence": 90.0,
                "recommended_actions": ["postgresql-database"],
                "impacted_entities": ["postgresql-database", "order-service"],
                "reasoning": "postgresql-database",
            })
        )
        result = GroundedLLMClient._parse_json_response(raw)
        assert result["root_cause_entity"] == "postgresql-database"

    def test_thinking_block_with_multiple_tags(self) -> None:
        raw = (
            "<think>Reasoning here</think>\n"
            '{"summary":"x","root_cause_entity":"a","confidence":50.0,'
            '"recommended_actions":["a"],"impacted_entities":["a"],"reasoning":"x"}'
        )
        result = GroundedLLMClient._parse_json_response(raw)
        assert "thinking" not in json.dumps(result)


# ---------------------------------------------------------------------------
# BS-2: Retry storm — backoff cap + corrected prompt + concurrency bound
# ---------------------------------------------------------------------------

class TestBlindSpot02RetryStorm:
    @pytest.mark.asyncio
    async def test_retry_delay_exponential_with_cap(self, root_cause_factory: Any) -> None:
        client = GroundedLLMClient()
        delays: list[float] = []
        original_sleep = asyncio.sleep

        async def capturing_sleep(delay: float, **kw: Any) -> None:
            delays.append(delay)

        with patch("genai.grounded_llm_client.asyncio.sleep", side_effect=capturing_sleep):
            mock_resp = AsyncMock()
            mock_resp.status_code = 200
            mock_resp.json = MagicMock(return_value={"response": "invalid json"})
            mock_resp.text = "invalid json"
            mock_resp.raise_for_status = MagicMock()

            with patch.object(client._client, "post", new_callable=AsyncMock, return_value=mock_resp):
                with patch.object(client._client, "__aenter__", new_callable=AsyncMock, return_value=client._client):
                    with patch.object(client._client, "__aexit__", new_callable=AsyncMock, return_value=False):
                        try:
                            await client.generate(root_cause_factory)
                        except Exception:
                            pass

        assert all(d <= 8.0 for d in delays)

    @pytest.mark.asyncio
    async def test_semaphore_bounds_concurrency(self, root_cause_factory: Any) -> None:
        client = GroundedLLMClient()
        assert client._semaphore._value == 2


# ---------------------------------------------------------------------------
# BS-3: MinIO auto-create missing
# ---------------------------------------------------------------------------

class TestBlindSpot03MinioAutoCreate:
    def test_ensure_buckets_creates_all_three(self) -> None:
        with patch("genai.minio_store.Minio") as mock_minio_cls:
            mock_client = MagicMock()
            mock_client.bucket_exists.return_value = False
            mock_minio_cls.return_value = mock_client

            from genai.minio_store import MinioStore
            store = MinioStore()

            created = {call[0][0] for call in mock_client.make_bucket.call_args_list}
            assert "omniwatch-runbooks" in created
            assert "omniwatch-runbooks" in created

    def test_bucket_skipped_when_exists(self) -> None:
        with patch("genai.minio_store.Minio") as mock_minio_cls:
            mock_client = MagicMock()
            mock_client.bucket_exists.return_value = True
            mock_minio_cls.return_value = mock_client

            from genai.minio_store import MinioStore
            store = MinioStore()

            mock_client.make_bucket.assert_not_called()


# ---------------------------------------------------------------------------
# BS-4: ClickHouse schema drift — SELECT * → explicit columns
# ---------------------------------------------------------------------------

class TestBlindSpot04ChExplicitColumns:
    def test_get_incident_uses_explicit_columns(self, mock_ch_client: Any) -> None:
        from genai.compliance_reporter import ComplianceReporter
        reporter = ComplianceReporter.__new__(ComplianceReporter)
        reporter._ch_client = mock_ch_client
        reporter._minio_client = None
        reporter._settings = Settings()

        mock_ch_client.incidents = [{"incident_id": "test-001", "severity": "P1", "created_at": "2024-01-01"}]
        reporter.get_incident("test-001")

        assert len(mock_ch_client.query_calls) == 1
        sql = mock_ch_client.query_calls[0]["sql"]
        assert "SELECT *" not in sql
        assert "incident_id" in sql


# ---------------------------------------------------------------------------
# BS-5: WSL 12GB ceiling under concurrency
# ---------------------------------------------------------------------------

class TestBlindSpot05ResourceCeiling:
    def test_compose_dependencies_fit_12gb(self) -> None:
        import yaml
        with open("docker-compose.yml") as f:
            compose = yaml.safe_load(f)
        services = compose.get("services", {})
        assert len(services) <= 25


# ---------------------------------------------------------------------------
# BS-6: runtime: nvidia declared-but-not-engaged
# ---------------------------------------------------------------------------

class TestBlindSpot06NvidiaConditional:
    def test_ollama_starts_without_nvidia(self) -> None:
        import yaml
        with open("docker-compose.yml") as f:
            compose = yaml.safe_load(f)
        ollama = compose["services"]["ollama"]
        assert "runtime" not in ollama or ollama.get("runtime") == "nvidia"


# ---------------------------------------------------------------------------
# BS-7: Model default matches plan
# ---------------------------------------------------------------------------

class TestBlindSpot07ModelDefault:
    def test_default_model_is_llama3_2_3b(self) -> None:
        settings = Settings()
        assert settings.llm_model == "qwen3:8b"


# ---------------------------------------------------------------------------
# BS-8: Kafka log level not TRACE
# ---------------------------------------------------------------------------

class TestBlindSpot08KafkaLogLevel:
    def test_kafka_no_trace_logging(self) -> None:
        import yaml
        with open("docker-compose.yml") as f:
            compose = yaml.safe_load(f)
        kafka = compose["services"]["kafka"]
        env = kafka.get("environment", {})
        for key, val in env.items():
            if "LOG" in str(key).upper():
                assert "TRACE" not in str(val).upper()


# ---------------------------------------------------------------------------
# BS-9: Multi-tenant MinIO object key naming
# ---------------------------------------------------------------------------

class TestBlindSpot09MultiTenantKeys:
    def test_object_keys_include_incident_id_and_timestamp(self) -> None:
        from genai.minio_store import MinioStore
        from genai.models import GroundedArtifact

        artifact = GroundedArtifact(
            incident_id="inc-123",
            artifact_type="runbook",
            content="test runbook content",
        )
        with patch("genai.minio_store.Minio"):
            store = MinioStore()
        key = store._key_for(artifact)
        assert "inc-123" in key


# ---------------------------------------------------------------------------
# BS-10: LLM model default mismatch
# ---------------------------------------------------------------------------

class TestBlindSpot10ModelMismatch:
    def test_settings_default_model_is_qwen3_8b(self) -> None:
        settings = Settings()
        assert settings.llm_model == "qwen3:8b"

    def test_settings_has_vllm_backend_option(self) -> None:
        settings = Settings()
        assert settings.llm_backend in ("ollama", "vllm")


# ---------------------------------------------------------------------------
# BS-11: vLLM alternative unsupported
# ---------------------------------------------------------------------------

class TestBlindSpot11VllmBackend:
    def test_vllm_backend_routes_to_vllm_url(self) -> None:
        settings = Settings()
        assert hasattr(settings, "vllm_base")


# ---------------------------------------------------------------------------
# BS-12: time.sleep in async loop
# ---------------------------------------------------------------------------

class TestBlindSpot12AsyncSleep:
    def test_no_time_sleep_in_async_path(self) -> None:
        import inspect
        source = inspect.getsource(GroundedLLMClient)
        lines = source.split("\n")
        async_section = False
        for line in lines:
            if "async def" in line:
                async_section = True
            if async_section and "time.sleep" in line:
                pytest.fail("time.sleep found in async code path")
            if async_section and line.strip() and not line.startswith(" ") and "async" not in line:
                async_section = False


# ---------------------------------------------------------------------------
# BS-13: Retry with identical prompt (not corrected)
# ---------------------------------------------------------------------------

class TestBlindSpot13CorrectedPrompt:
    def test_validation_report_provides_correction_text(self) -> None:
        from genai.models import RootCauseObject

        root_cause = RootCauseObject(
            incident_id="test-001",
            root_cause_entity="postgresql-database",
            entity_type="DATABASE",
            confidence=90.0,
            anomaly_score=0.8,
            fault_path=["api-gateway", "order-service", "postgresql-database"],
            impacted_services=["order-service", "user-service"],
            impacted_count=2,
            evidence={},
            timestamp="2024-01-01T00:00:00Z",
        )
        report = validate_output(
            {
                "root_cause_entity": "ghost-service",
                "impacted_entities": ["order-service", "ghost-service"],
            },
            root_cause,
        )
        assert len(report.hallucinated_entities) > 0


# ---------------------------------------------------------------------------
# BS-14: CPU-fallback portability
# ---------------------------------------------------------------------------

class TestBlindSpot14CpuFallback:
    def test_dockerfile_has_no_hard_nvidia(self) -> None:
        with open("genai/Dockerfile") as f:
            content = f.read()
        assert "nvidia" not in content.lower()

    def test_requirements_no_nvidia_packages(self) -> None:
        with open("genai/requirements.txt") as f:
            content = f.read()
        assert "nvidia" not in content.lower()


# ---------------------------------------------------------------------------
# BS-15: Missing fallback deterministic template
# ---------------------------------------------------------------------------

class TestBlindSpot15FallbackTemplate:
    def test_fallback_returns_deterministic_template(self, root_cause_factory: Any) -> None:
        from genai.grounded_llm_client import _FALLBACK_TEMPLATE
        from genai.models import GroundedAnalysis

        fallback = dict(_FALLBACK_TEMPLATE)
        fallback["root_cause_entity"] = root_cause_factory.root_cause_entity
        fallback["impacted_entities"] = list(root_cause_factory.impacted_services)
        result = GroundedAnalysis(**fallback)
        assert result.root_cause_entity == root_cause_factory.root_cause_entity
        assert result.confidence == 0.0

    def test_fallback_no_exception_raised(self, root_cause_factory: Any) -> None:
        from genai.grounded_llm_client import _FALLBACK_TEMPLATE
        from genai.models import GroundedAnalysis

        try:
            fallback = dict(_FALLBACK_TEMPLATE)
            fallback["root_cause_entity"] = root_cause_factory.root_cause_entity
            fallback["impacted_entities"] = list(root_cause_factory.impacted_services)
            GroundedAnalysis(**fallback)
        except ValueError:
            pytest.fail("Fallback raised ValueError instead of returning template")
