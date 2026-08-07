"""
OmniWatch — Generative AI Layer
Component: Test Fixtures (conftest.py)
Phase: 10
Purpose: Shared pytest fixtures for GenAI tests — mock Ollama/vLLM /api/generate,
         mock Kafka consumer/producer, mock MinIO client, and RootCauseObject factory.
         All fixtures run offline with no live service dependencies.
Inputs: None (self-contained mocks)
Outputs: Fixtures consumed by test files in tests/phase-10-genai/
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from genai.models import RootCauseObject


# ---------------------------------------------------------------------------
# RootCauseObject factory
# ---------------------------------------------------------------------------

@pytest.fixture
def root_cause_factory() -> RootCauseObject:
    """Return a valid RootCauseObject matching AGENTS.md contract."""
    return RootCauseObject(
        incident_id=str(uuid.uuid4()),
        root_cause_entity="postgresql-database",
        entity_type="DATABASE_NODE",
        confidence=92.0,
        anomaly_score=0.85,
        fault_path=[
            "postgresql-database",
            "order-service",
            "api-gateway",
        ],
        impacted_services=["order-service", "api-gateway"],
        impacted_count=3,
        evidence={
            "log_snippets": ["error: connection refused"],
            "metrics": {"cpu_usage": "95.2"},
            "anomaly_timeline": [],
        },
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@pytest.fixture
def root_cause_dict() -> dict[str, Any]:
    """Return a valid RootCauseObject as a plain dict."""
    return {
        "incident_id": str(uuid.uuid4()),
        "root_cause_entity": "postgresql-database",
        "entity_type": "DATABASE_NODE",
        "confidence": 92.0,
        "anomaly_score": 0.85,
        "fault_path": ["postgresql-database", "order-service", "api-gateway"],
        "impacted_services": ["order-service", "api-gateway"],
        "impacted_count": 3,
        "evidence": {
            "log_snippets": ["error: connection refused"],
            "metrics": {"cpu_usage": "95.2"},
            "anomaly_timeline": [],
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Grounded LLM output factories
# ---------------------------------------------------------------------------

@pytest.fixture
def grounded_output() -> dict[str, Any]:
    """Return a valid grounded LLM output (all entities exist in RootCauseObject)."""
    return {
        "summary": "postgresql-database",
        "root_cause_entity": "postgresql-database",
        "confidence": 90.0,
        "recommended_actions": ["postgresql-database", "order-service"],
        "impacted_entities": ["postgresql-database", "order-service", "api-gateway"],
        "reasoning": "postgresql-database",
    }


@pytest.fixture
def hallucinated_output() -> dict[str, Any]:
    """Return an LLM output with hallucinated entities (not in RootCauseObject)."""
    return {
        "summary": "The redis-cache failure caused a cascade through postgresql-database.",
        "root_cause_entity": "redis-cache",
        "confidence": 85.0,
        "recommended_actions": ["restart_service redis-cache", "clear_cache redis-cache"],
        "impacted_entities": ["redis-cache", "postgresql-database"],
        "reasoning": "redis-cache timeout triggered connection pool exhaustion on postgresql-database.",
    }


# ---------------------------------------------------------------------------
# Mock Ollama /api/generate
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_ollama_response() -> dict[str, Any]:
    """Return a mock Ollama /api/generate response body."""
    grounded = {
        "summary": "postgresql-database",
        "root_cause_entity": "postgresql-database",
        "confidence": 90.0,
        "recommended_actions": ["postgresql-database", "order-service"],
        "impacted_entities": ["postgresql-database", "order-service"],
        "reasoning": "postgresql-database",
    }
    return {
        "model": "qwen3:8b",
        "response": json.dumps(grounded),
        "done": True,
        "total_duration": 5000000000,
        "eval_count": 150,
    }


@pytest.fixture
def mock_ollama_hallucinated_response() -> dict[str, Any]:
    """Return a mock Ollama response with hallucinated entities."""
    hallucinated = {
        "summary": "The redis-cache failure caused a cascade.",
        "root_cause_entity": "redis-cache",
        "confidence": 85.0,
        "recommended_actions": ["restart_service redis-cache"],
        "impacted_entities": ["redis-cache", "postgresql-database"],
        "reasoning": "redis-cache timeout triggered connection pool exhaustion.",
    }
    return {
        "model": "qwen3:8b",
        "response": json.dumps(hallucinated),
        "done": True,
    }


# ---------------------------------------------------------------------------
# Mock ClickHouse client
# ---------------------------------------------------------------------------

class MockClickHouseClient:
    """Mock ClickHouse client that returns configurable incident data."""

    def __init__(self) -> None:
        self.incidents: list[dict[str, Any]] = []
        self.query_calls: list[dict[str, Any]] = []

    def query(self, sql: str, parameters: dict[str, Any] | None = None) -> MagicMock:
        """Mock query that returns configured incidents."""
        self.query_calls.append({"sql": sql, "parameters": parameters})
        result = MagicMock()
        result.column_names = [
            "incident_id", "severity", "business_impact_score",
            "root_cause_entity", "entity_type", "confidence",
            "fault_path", "impacted_services", "status",
            "deduplicated_count", "sla_breach_risk", "assigned_to", "created_at",
        ]
        # Filter by incident_id if parameter provided
        if parameters and "id" in parameters:
            filtered = [i for i in self.incidents if i.get("incident_id") == parameters["id"]]
            result.result_rows = [list(inc.values()) for inc in filtered]
        else:
            result.result_rows = [list(inc.values()) for inc in self.incidents]
        return result

    def command(self, sql: str) -> Any:
        """Mock command (health check)."""
        return True

    def close(self) -> None:
        """Mock close."""


@pytest.fixture
def mock_ch_client() -> MockClickHouseClient:
    """Provide a fresh MockClickHouseClient."""
    return MockClickHouseClient()


# ---------------------------------------------------------------------------
# Mock MinIO client
# ---------------------------------------------------------------------------

class MockMinioClient:
    """Mock MinIO client that returns configurable audit log data."""

    def __init__(self) -> None:
        self.buckets: set[str] = {"omniwatch-audit-logs"}
        self.objects: dict[str, bytes] = {}
        self.put_calls: list[dict[str, Any]] = []
        self.list_calls: list[str] = []

    def bucket_exists(self, bucket: str) -> bool:
        """Check if bucket exists."""
        return bucket in self.buckets

    def list_objects(self, bucket: str, prefix: str = "") -> list[MagicMock]:
        """List objects with prefix."""
        self.list_calls.append(prefix)
        results = []
        for key in self.objects:
            if key.startswith(prefix):
                obj = MagicMock()
                obj.object_name = key
                obj.size = len(self.objects[key])
                obj.last_modified = datetime.now(timezone.utc)
                results.append(obj)
        return results

    def put_object(
        self, bucket: str, object_name: str, data: Any, length: int, content_type: str = ""
    ) -> MagicMock:
        """Put object into bucket."""
        content = data.read() if hasattr(data, "read") else data
        self.objects[object_name] = content if isinstance(content, bytes) else content.encode()
        self.put_calls.append({
            "bucket": bucket,
            "object_name": object_name,
            "length": length,
        })
        result = MagicMock()
        result.object_name = object_name
        return result


@pytest.fixture
def mock_minio_client() -> MockMinioClient:
    """Provide a fresh MockMinioClient."""
    return MockMinioClient()


# ---------------------------------------------------------------------------
# Incident record factory
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_incident() -> dict[str, Any]:
    """Return a sample incident record matching ClickHouse schema."""
    return {
        "incident_id": "test-incident-001",
        "severity": "P1",
        "business_impact_score": 88.5,
        "root_cause_entity": "postgresql-database",
        "entity_type": "DATABASE_NODE",
        "confidence": 92.0,
        "fault_path": '["postgresql-database", "order-service", "api-gateway"]',
        "impacted_services": '["order-service", "api-gateway"]',
        "status": "OPEN",
        "deduplicated_count": 1,
        "sla_breach_risk": "HIGH",
        "assigned_to": "auto-remediation",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Mock vLLM /v1/completions response
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_vllm_response() -> dict[str, Any]:
    """Return a mock vLLM /v1/completions response body."""
    grounded = {
        "summary": "postgresql-database",
        "root_cause_entity": "postgresql-database",
        "confidence": 90.0,
        "recommended_actions": ["postgresql-database", "order-service"],
        "impacted_entities": ["postgresql-database", "order-service"],
        "reasoning": "postgresql-database",
    }
    return {
        "id": "cmpl-test-001",
        "object": "text_completion",
        "created": 1700000000,
        "model": "qwen2.5:7b",
        "choices": [
            {
                "text": json.dumps(grounded),
                "index": 0,
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": 100,
            "completion_tokens": 150,
            "total_tokens": 250,
        },
    }


# ---------------------------------------------------------------------------
# Mock Kafka message
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_kafka_message() -> dict[str, Any]:
    """Return a mock Kafka message for omniwatch.incidents.created."""
    return {
        "topic": "omniwatch.incidents.created",
        "partition": 0,
        "offset": 12345,
        "key": "test-incident-001",
        "value": json.dumps({
            "incident_id": "test-incident-001",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "severity": "P1",
            "business_impact_score": 88.5,
            "root_cause": {
                "incident_id": "test-incident-001",
                "root_cause_entity": "postgresql-database",
                "entity_type": "DATABASE_NODE",
                "confidence": 92.0,
                "anomaly_score": 0.85,
                "fault_path": ["postgresql-database", "order-service", "api-gateway"],
                "impacted_services": ["order-service", "api-gateway"],
                "impacted_count": 3,
                "evidence": {
                    "log_snippets": ["error: connection refused"],
                    "metrics": {"cpu_usage": "95.2"},
                    "anomaly_timeline": [],
                },
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            "related_anomalies": [],
            "deduplicated_count": 1,
            "sla_breach_risk": "HIGH",
            "assigned_to": "auto-remediation",
            "status": "OPEN",
        }),
        "timestamp": datetime.now(timezone.utc),
    }


@pytest.fixture
def mock_remediation_action() -> dict[str, Any]:
    """Return a mock Kafka message for omniwatch.remediation.actions."""
    return {
        "topic": "omniwatch.remediation.actions",
        "partition": 0,
        "offset": 12346,
        "key": "test-incident-001",
        "value": json.dumps({
            "action_type": "restart_service",
            "entity_id": "postgresql-database",
            "success": True,
            "output": "Service restarted successfully",
            "error": None,
            "execution_time_seconds": 12.5,
            "executed_at": datetime.now(timezone.utc).isoformat(),
            "triggered_by": "auto",
            "incident_id": "test-incident-001",
        }),
        "timestamp": datetime.now(timezone.utc),
    }


# ---------------------------------------------------------------------------
# Mock MinIO auto-create
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_minio_auto_create() -> MockMinioClient:
    """Return a MockMinioClient with all 3 required buckets pre-created."""
    client = MockMinioClient()
    client.buckets = {
        "omniwatch-runbooks",
        "omniwatch-audit-logs",
        "omniwatch-incidents",
    }
    return client
