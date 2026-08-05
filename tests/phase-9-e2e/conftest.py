"""
OmniWatch — Orchestration + Policy
Component: E2E Test Fixtures (conftest.py)
Phase: 9
Purpose: Shared pytest fixtures for orchestration tests — mock OPA client,
         mock action executor, mock Kafka producer, and incident factory.
         All fixtures run offline with no live service dependencies.
Inputs: None (self-contained mocks)
Outputs: Fixtures consumed by test files in tests/phase-9-e2e/
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import pytest


# ---------------------------------------------------------------------------
# Fake OPA client — unit-testable policy decision mock
# ---------------------------------------------------------------------------

class FakeOPAClient:
    """Lightweight OPA stand-in for unit tests.

    Returns a configurable ``OrchestrationDecision`` for each ``decide()``
    call.  Default behaviour is ``allow=True, needs_approval=False``.

    Usage in tests::

        opa = FakeOPAClient()
        opa.allow = False           # override for deny scenario
        opa.needs_approval = True   # override for approval-required scenario
        decision = opa.decide(incident_dict, "restart")
    """

    def __init__(self) -> None:
        self.allow: bool = True
        self.needs_approval: bool = False
        self.reason: str = ""
        self.calls: list[dict[str, Any]] = []

    def decide(
        self,
        incident: dict[str, Any],
        action_type: str,
    ) -> dict[str, Any]:
        """Evaluate the action against (mocked) OPA policies.

        Args:
            incident: IncidentRecord-like dict.
            action_type: Proposed action type string.

        Returns:
            Dict matching OrchestrationDecision wire format.
        """
        self.calls.append({"incident": incident, "action_type": action_type})
        return {
            "result": {
                "allow": self.allow,
                "needs_approval": self.needs_approval,
                "reason": self.reason,
            }
        }


# ---------------------------------------------------------------------------
# Mock action executor
# ---------------------------------------------------------------------------

class MockActionExecutor:
    """Mock executor that records calls and returns configurable results.

    Default behaviour: ``success=True``.  Override per test::

        executor = MockActionExecutor()
        executor.success = False
        executor.error = "connection refused"
        result = executor.execute(action_def, incident)
    """

    def __init__(self) -> None:
        self.success: bool = True
        self.output: str = "action completed"
        self.error: str | None = None
        self.execution_time: float = 0.5
        self.calls: list[dict[str, Any]] = []

    def execute(
        self,
        action_definition: dict[str, Any],
        incident: dict[str, Any],
    ) -> dict[str, Any]:
        """Execute a (mocked) remediation action.

        Args:
            action_definition: Dict describing the action to execute.
            incident: IncidentRecord-like dict providing context.

        Returns:
            Dict with ActionResult fields (minus action_id, executed_at
            which are added by the caller).
        """
        self.calls.append({
            "action_definition": action_definition,
            "incident": incident,
        })
        return {
            "success": self.success,
            "output": self.output,
            "error": self.error,
            "execution_time_seconds": self.execution_time,
        }


# ---------------------------------------------------------------------------
# Mock Kafka producer
# ---------------------------------------------------------------------------

class MockKafkaProducer:
    """Mock producer that records published ActionResult messages.

    Usage::

        producer = MockKafkaProducer()
        producer.publish_action_result(action_result_dict)
        assert len(producer.published) == 1
    """

    def __init__(self) -> None:
        self.published: list[dict[str, Any]] = []

    def publish_action_result(self, action_result: dict[str, Any]) -> None:
        """Record a published ActionResult for assertion."""
        self.published.append(action_result)


# ---------------------------------------------------------------------------
# Pytest fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_opa_server() -> FakeOPAClient:
    """Provide a fresh FakeOPAClient with default allow=True."""
    return FakeOPAClient()


@pytest.fixture
def mock_executor() -> MockActionExecutor:
    """Provide a fresh MockActionExecutor with default success=True."""
    return MockActionExecutor()


@pytest.fixture
def mock_producer() -> MockKafkaProducer:
    """Provide a fresh MockKafkaProducer with empty published list."""
    return MockKafkaProducer()


@pytest.fixture
def incident_factory() -> dict[str, Any]:
    """Return a valid IncidentRecord-like dict (plain dict, no Pydantic deps).

    Matches the AGENTS.md IncidentRecord contract:
    incident_id, created_at, severity, business_impact_score, root_cause,
    related_anomalies, deduplicated_count, sla_breach_risk, assigned_to, status.
    """
    return {
        "incident_id": str(uuid.uuid4()),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "severity": "P1",
        "business_impact_score": 88.5,
        "root_cause": {
            "incident_id": str(uuid.uuid4()),
            "root_cause_entity": "postgresql-database",
            "entity_type": "DATABASE_NODE",
            "confidence": 92.0,
            "anomaly_score": 0.85,
            "fault_path": [
                "postgresql-database",
                "order-service",
                "api-gateway",
            ],
            "impacted_services": ["order-service", "api-gateway"],
            "impacted_count": 3,
            "evidence": {
                "log_snippets": ["error: connection refused"],
                "metrics": {"cpu_usage": 95.2},
                "anomaly_timeline": [],
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
        "related_anomalies": [],
        "deduplicated_count": 1,
        "sla_breach_risk": "HIGH",
        "assigned_to": "auto-remediation",
        "status": "OPEN",
    }
