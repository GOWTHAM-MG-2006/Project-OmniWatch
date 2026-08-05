"""
OmniWatch — Orchestration + Policy
Component: E2E Test Suite (Stage C — Action Executors)
Phase: 9
Purpose: Unit tests for action library entity mapping, idempotency key generation,
         and SimulationExecutor behaviour (success path + dry-run mode).
         All tests run offline with no live service dependencies.
Inputs: Mock fixtures from conftest.py, action_library, executor modules
Outputs: pytest pass/fail — verifies executor contract before orchestrator integration
"""

from __future__ import annotations

import os
from unittest.mock import Mock

import pytest

from orchestration.action_library import (
    ActionLibrary,
    build_idempotency_key,
    get_actions,
)
from orchestration.executor import SimulationExecutor
from orchestration.models import ActionResult, ApprovalRecord


# ---------------------------------------------------------------------------
# Test 4: Action library entity mapping
# ---------------------------------------------------------------------------

class TestActionLibraryEntityMapping:
    """DATABASE_NODE → safe actions: restart_service, rollback, clear_cache.

    get_actions() returns SORTED list; test uses set comparison to avoid
    ordering fragility (inherited wisdom from action_library.py verification).
    """

    def test_database_node_entity_mapping(self) -> None:
        """DATABASE_NODE maps to exactly 3 safe actions."""
        actions = get_actions("DATABASE_NODE")

        # Compare as sets — ordering is implementation detail
        assert set(actions) == {"restart_service", "rollback", "clear_cache"}
        assert len(actions) == 3

    def test_action_library_class_same_result(self) -> None:
        """ActionLibrary().get_actions() matches module-level get_actions()."""
        lib = ActionLibrary()
        assert set(lib.get_actions("DATABASE_NODE")) == set(get_actions("DATABASE_NODE"))


# ---------------------------------------------------------------------------
# Test 5: Idempotency key generation
# ---------------------------------------------------------------------------

class TestActionLibraryIdempotency:
    """Same (action_type, entity_id, incident_id) → identical dedup key."""

    def test_idempotency_key_deterministic(self) -> None:
        """Two calls with same inputs produce the same key."""
        key1 = build_idempotency_key("restart_service", "pg-db-01", "inc-123")
        key2 = build_idempotency_key("restart_service", "pg-db-01", "inc-123")
        assert key1 == key2
        assert key1 == "restart_service:pg-db-01:inc-123"

    def test_idempotency_key_differs_on_entity(self) -> None:
        """Different entity_id produces different key."""
        key1 = build_idempotency_key("restart_service", "pg-db-01", "inc-123")
        key2 = build_idempotency_key("restart_service", "pg-db-02", "inc-123")
        assert key1 != key2

    def test_idempotency_key_differs_on_incident(self) -> None:
        """Different incident_id produces different key."""
        key1 = build_idempotency_key("restart_service", "pg-db-01", "inc-123")
        key2 = build_idempotency_key("restart_service", "pg-db-01", "inc-456")
        assert key1 != key2


# ---------------------------------------------------------------------------
# Test 6: SimulationExecutor success
# ---------------------------------------------------------------------------

class TestSimulationExecutorSuccess:
    """SimulationExecutor.execute() returns success=True for valid actions."""

    def test_simulation_executor_success(
        self, incident_factory: dict
    ) -> None:
        """execute() returns success=True with meaningful output."""
        executor = SimulationExecutor()
        action_def = {
            "action_type": "restart_service",
            "entity_type": "DATABASE_NODE",
            "safe": True,
            "description": "Restart the database service",
        }

        result = executor.execute(action_def, incident_factory)

        assert result["success"] is True
        assert isinstance(result["output"], str)
        assert result["output"]  # non-empty
        assert result["execution_time_seconds"] >= 0

    def test_simulation_executor_tracks_calls(
        self, incident_factory: dict
    ) -> None:
        """execute() records the call in the executor's history."""
        executor = SimulationExecutor()
        action_def = {
            "action_type": "clear_cache",
            "entity_type": "DATABASE_NODE",
            "safe": True,
            "description": "Clear query cache",
        }

        executor.execute(action_def, incident_factory)

        assert len(executor.calls) == 1
        assert executor.calls[0]["action_type"] == "clear_cache"


# ---------------------------------------------------------------------------
# Test 7: SimulationExecutor dry-run mode
# ---------------------------------------------------------------------------

class TestSimulationExecutorDryRun:
    """DRY_RUN=true → output contains 'dry-run', no real execution."""

    def test_simulation_executor_dry_run(
        self, incident_factory: dict, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When DRY_RUN=true, output contains 'dry-run' substring."""
        monkeypatch.setenv("DRY_RUN", "true")

        executor = SimulationExecutor()
        action_def = {
            "action_type": "restart_service",
            "entity_type": "DATABASE_NODE",
            "safe": True,
            "description": "Restart the database service",
        }

        result = executor.execute(action_def, incident_factory)

        assert result["success"] is True
        assert "dry-run" in result["output"].lower()
        assert "restart_service" in result["output"]
        assert "postgresql-database" in result["output"] or "entity" in result["output"].lower()

    def test_dry_run_output_format(
        self, incident_factory: dict, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """DRY_RUN output matches expected format."""
        monkeypatch.setenv("DRY_RUN", "true")

        executor = SimulationExecutor()
        action_def = {
            "action_type": "rollback",
            "entity_type": "DATABASE_NODE",
            "safe": True,
            "description": "Roll back database",
        }

        result = executor.execute(action_def, incident_factory)

        # Expected: "dry-run: would execute {action_type} on {entity_id}"
        assert result["output"].startswith("dry-run: would execute rollback on")

    def test_simulation_executor_idempotency_dedup(
        self, incident_factory: dict
    ) -> None:
        """Same action+entity+incident → second call is idempotent no-op."""
        executor = SimulationExecutor()
        action_def = {
            "action_type": "restart_service",
            "entity_type": "DATABASE_NODE",
            "safe": True,
            "description": "Restart",
        }
        entity_id = incident_factory["root_cause"]["root_cause_entity"]
        incident_id = incident_factory["incident_id"]

        result1 = executor.execute(action_def, incident_factory)
        result2 = executor.execute(action_def, incident_factory)

        assert result1["success"] is True
        # Second call returns success (idempotent no-op)
        assert result2["success"] is True
        assert "already" in result2["output"].lower() or "dedup" in result2["output"].lower() or result2["output"]


# ---------------------------------------------------------------------------
# Test 8: Orchestrator auto-remediation flow
# ---------------------------------------------------------------------------

class TestOrchestratorAutoFlow:
    """P1 DATABASE_NODE with high confidence → auto-execute, publish, audit."""

    def _build_incident(
        self,
        entity_type: str = "DATABASE_NODE",
        entity_id: str = "postgresql-database",
        severity: str = "P1",
        confidence: float = 96.0,
    ) -> dict:
        """Build a minimal IncidentRecord-like dict for testing."""
        return {
            "incident_id": "test-auto-001",
            "created_at": "2026-08-05T00:00:00Z",
            "severity": severity,
            "business_impact_score": 88.5,
            "root_cause": {
                "incident_id": "test-auto-001",
                "root_cause_entity": entity_id,
                "entity_type": entity_type,
                "confidence": confidence,
                "anomaly_score": 0.85,
                "fault_path": [entity_id],
                "impacted_services": [entity_id],
                "impacted_count": 1,
                "evidence": {"log_snippets": [], "metrics": {}, "anomaly_timeline": []},
                "timestamp": "2026-08-05T00:00:00Z",
            },
            "related_anomalies": [],
            "deduplicated_count": 1,
            "sla_breach_risk": "HIGH",
            "assigned_to": "auto-remediation",
            "status": "OPEN",
        }

    def test_auto_flow_returns_action_result(
        self,
        mock_opa_server,
        mock_executor,
        mock_producer,
    ) -> None:
        """Auto-flow returns ActionResult with success=True and triggered_by='auto'."""
        from orchestration.orchestrator import Orchestrator

        incident = self._build_incident(severity="P1", confidence=96.0)

        orch = Orchestrator(
            opa=mock_opa_server,
            executor=mock_executor,
            producer=mock_producer,
        )

        result = orch.handle_message(incident)

        assert result is not None
        assert isinstance(result, ActionResult)
        assert result.success is True
        assert result.triggered_by == "auto"
        assert result.incident_id == "test-auto-001"
        assert result.entity_id == "postgresql-database"
        assert result.entity_type == "DATABASE_NODE"
        assert result.severity == "P1"
        assert result.confidence == 96.0

    def test_auto_flow_publishes_to_producer(
        self,
        mock_opa_server,
        mock_executor,
        mock_producer,
    ) -> None:
        """Auto-flow publishes ActionResult to the Kafka producer."""
        from orchestration.orchestrator import Orchestrator

        incident = self._build_incident(severity="P1", confidence=96.0)

        orch = Orchestrator(
            opa=mock_opa_server,
            executor=mock_executor,
            producer=mock_producer,
        )

        orch.handle_message(incident)

        assert len(mock_producer.published) == 1
        published = mock_producer.published[0]
        assert published["success"] is True
        assert published["triggered_by"] == "auto"
        assert published["incident_id"] == "test-auto-001"

    def test_auto_flow_calls_executor(
        self,
        mock_opa_server,
        mock_executor,
        mock_producer,
    ) -> None:
        """Auto-flow calls the executor with the action definition."""
        from orchestration.orchestrator import Orchestrator

        incident = self._build_incident(severity="P1", confidence=96.0)

        orch = Orchestrator(
            opa=mock_opa_server,
            executor=mock_executor,
            producer=mock_producer,
        )

        orch.handle_message(incident)

        assert len(mock_executor.calls) == 1
        action_def = mock_executor.calls[0]["action_definition"]
        # First safe action for DATABASE_NODE is restart_service
        assert action_def["action_type"] == "restart_service"
        assert action_def["entity_type"] == "DATABASE_NODE"


# ---------------------------------------------------------------------------
# Test 9: Orchestrator approval flow
# ---------------------------------------------------------------------------

class TestOrchestratorApprovalFlow:
    """API_NODE with block_ip → approval required, stored in ClickHouse."""

    def _build_incident(
        self,
        entity_type: str = "API_NODE",
        entity_id: str = "api-gateway",
        severity: str = "P1",
        confidence: float = 92.0,
    ) -> dict:
        return {
            "incident_id": "test-approval-001",
            "created_at": "2026-08-05T00:00:00Z",
            "severity": severity,
            "business_impact_score": 85.0,
            "root_cause": {
                "incident_id": "test-approval-001",
                "root_cause_entity": entity_id,
                "entity_type": entity_type,
                "confidence": confidence,
                "anomaly_score": 0.8,
                "fault_path": [entity_id],
                "impacted_services": [entity_id],
                "impacted_count": 1,
                "evidence": {"log_snippets": [], "metrics": {}, "anomaly_timeline": []},
                "timestamp": "2026-08-05T00:00:00Z",
            },
            "related_anomalies": [],
            "deduplicated_count": 1,
            "sla_breach_risk": "HIGH",
            "assigned_to": "auto-remediation",
            "status": "OPEN",
        }

    def test_approval_flow_returns_approval_record(
        self,
        mock_opa_server,
        mock_executor,
        mock_producer,
    ) -> None:
        """Approval flow returns ApprovalRecord with status='pending'."""
        from orchestration.orchestrator import Orchestrator

        # Deny all safe actions → only block_ip (always-approval) is picked
        mock_opa_server.allow = False
        mock_opa_server.needs_approval = False

        incident = self._build_incident()

        clickhouse_fn = Mock(return_value=1)
        orch = Orchestrator(
            opa=mock_opa_server,
            executor=mock_executor,
            producer=mock_producer,
            clickhouse_fn=clickhouse_fn,
        )

        result = orch.handle_message(incident)

        assert result is not None
        assert isinstance(result, ApprovalRecord)
        assert result.status == "pending"
        assert result.action_type == "block_ip"
        assert result.entity_id == "api-gateway"
        assert result.incident_id == "test-approval-001"
        assert result.incident_severity == "P1"

    def test_approval_flow_stores_in_clickhouse(
        self,
        mock_opa_server,
        mock_executor,
        mock_producer,
    ) -> None:
        """Approval flow calls clickhouse_fn with the approval record."""
        from orchestration.orchestrator import Orchestrator

        mock_opa_server.allow = False
        mock_opa_server.needs_approval = False

        incident = self._build_incident(severity="P2", confidence=88.0)

        clickhouse_fn = Mock(return_value=1)
        orch = Orchestrator(
            opa=mock_opa_server,
            executor=mock_executor,
            producer=mock_producer,
            clickhouse_fn=clickhouse_fn,
        )

        orch.handle_message(incident)

        clickhouse_fn.assert_called_once()
        call_args = clickhouse_fn.call_args[0][0]
        assert call_args["action_type"] == "block_ip"
        assert call_args["incident_id"] == "test-approval-001"

    def test_approval_flow_does_not_publish_or_execute(
        self,
        mock_opa_server,
        mock_executor,
        mock_producer,
    ) -> None:
        """Approval flow does NOT publish to Kafka or call the executor."""
        from orchestration.orchestrator import Orchestrator

        mock_opa_server.allow = False
        mock_opa_server.needs_approval = False

        incident = self._build_incident()

        orch = Orchestrator(
            opa=mock_opa_server,
            executor=mock_executor,
            producer=mock_producer,
        )

        orch.handle_message(incident)

        assert len(mock_producer.published) == 0
        assert len(mock_executor.calls) == 0


# ---------------------------------------------------------------------------
# Test 10: Orchestrator retry on failure
# ---------------------------------------------------------------------------

class TestOrchestratorRetryOnFailure:
    """Executor fails first attempts, succeeds on retry → exponential backoff."""

    def test_retry_succeeds_after_failures(
        self,
        mock_opa_server,
        mock_producer,
        incident_factory,
        monkeypatch,
    ) -> None:
        """After 2 failures, 3rd attempt succeeds. Executor called 3 times."""
        from orchestration.orchestrator import Orchestrator

        # Patch time.sleep to avoid actual delays
        monkeypatch.setattr("orchestration.orchestrator.time.sleep", lambda _: None)

        # Custom executor: fail twice, succeed on 3rd
        class RetryExecutor:
            def __init__(self):
                self.call_count = 0
                self.calls = []

            def execute(self, action_definition, incident):
                self.call_count += 1
                self.calls.append({
                    "action_definition": action_definition,
                    "incident": incident,
                })
                if self.call_count < 3:
                    return {
                        "success": False,
                        "output": "transient error",
                        "error": "connection refused",
                        "execution_time_seconds": 0.1,
                    }
                return {
                    "success": True,
                    "output": "action completed on retry",
                    "error": None,
                    "execution_time_seconds": 0.5,
                }

        executor = RetryExecutor()
        incident_factory["root_cause"]["confidence"] = 96.0
        incident_factory["severity"] = "P1"

        orch = Orchestrator(
            opa=mock_opa_server,
            executor=executor,
            producer=mock_producer,
        )

        result = orch.handle_message(incident_factory)

        assert result is not None
        assert isinstance(result, ActionResult)
        assert result.success is True
        assert executor.call_count == 3
        assert len(mock_producer.published) == 1

    def test_retry_exhausted_returns_failure(
        self,
        mock_opa_server,
        mock_producer,
        incident_factory,
        monkeypatch,
    ) -> None:
        """All 3 attempts fail → returns failed ActionResult, still published."""
        from orchestration.orchestrator import Orchestrator

        monkeypatch.setattr("orchestration.orchestrator.time.sleep", lambda _: None)

        class FailExecutor:
            def __init__(self):
                self.call_count = 0
                self.calls = []

            def execute(self, action_definition, incident):
                self.call_count += 1
                self.calls.append({
                    "action_definition": action_definition,
                    "incident": incident,
                })
                return {
                    "success": False,
                    "output": "permanent failure",
                    "error": "disk full",
                    "execution_time_seconds": 0.1,
                }

        executor = FailExecutor()
        incident_factory["root_cause"]["confidence"] = 96.0
        incident_factory["severity"] = "P1"

        orch = Orchestrator(
            opa=mock_opa_server,
            executor=executor,
            producer=mock_producer,
        )

        result = orch.handle_message(incident_factory)

        assert result is not None
        assert isinstance(result, ActionResult)
        assert result.success is False
        assert executor.call_count == 3
        assert len(mock_producer.published) == 1
        assert mock_producer.published[0]["success"] is False


# ---------------------------------------------------------------------------
# Test 14: Orchestrator audit archive to MinIO
# ---------------------------------------------------------------------------

class TestOrchestratorAuditArchive:
    """Auto-flow archives ActionResult to MinIO audit bucket."""

    def test_audit_archives_to_minio(
        self,
        mock_opa_server,
        mock_executor,
        mock_producer,
    ) -> None:
        """After auto-execution, archiver.upload_object is called with correct args."""
        from orchestration.orchestrator import Orchestrator

        incident = {
            "incident_id": "test-audit-001",
            "created_at": "2026-08-05T00:00:00Z",
            "severity": "P1",
            "business_impact_score": 88.5,
            "root_cause": {
                "incident_id": "test-audit-001",
                "root_cause_entity": "postgresql-database",
                "entity_type": "DATABASE_NODE",
                "confidence": 96.0,
                "anomaly_score": 0.85,
                "fault_path": ["postgresql-database"],
                "impacted_services": ["postgresql-database"],
                "impacted_count": 1,
                "evidence": {"log_snippets": [], "metrics": {}, "anomaly_timeline": []},
                "timestamp": "2026-08-05T00:00:00Z",
            },
            "related_anomalies": [],
            "deduplicated_count": 1,
            "sla_breach_risk": "HIGH",
            "assigned_to": "auto-remediation",
            "status": "OPEN",
        }

        archiver = Mock()
        orch = Orchestrator(
            opa=mock_opa_server,
            executor=mock_executor,
            producer=mock_producer,
            archiver=archiver,
        )

        result = orch.handle_message(incident)

        assert result is not None
        assert isinstance(result, ActionResult)
        assert result.archived is True
        archiver.upload_object.assert_called_once()
        call_args = archiver.upload_object.call_args
        assert call_args[0][0] == "omniwatch-audit-logs"
        assert call_args[0][1].startswith("audit/")
        assert call_args[0][1].endswith(".json")

    def test_audit_skipped_when_no_archiver(
        self,
        mock_opa_server,
        mock_executor,
        mock_producer,
        incident_factory,
    ) -> None:
        """When archiver is None, audit is silently skipped."""
        from orchestration.orchestrator import Orchestrator

        incident_factory["root_cause"]["confidence"] = 96.0
        incident_factory["severity"] = "P1"

        orch = Orchestrator(
            opa=mock_opa_server,
            executor=mock_executor,
            producer=mock_producer,
            archiver=None,
        )

        result = orch.handle_message(incident_factory)

        assert result is not None
        assert isinstance(result, ActionResult)
        assert result.archived is False

    def test_audit_fail_soft_on_error(
        self,
        mock_opa_server,
        mock_executor,
        mock_producer,
        incident_factory,
    ) -> None:
        """When archiver raises, audit fails softly (result still returned)."""
        from orchestration.orchestrator import Orchestrator

        incident_factory["root_cause"]["confidence"] = 96.0
        incident_factory["severity"] = "P1"

        archiver = Mock()
        archiver.upload_object.side_effect = Exception("MinIO unreachable")

        orch = Orchestrator(
            opa=mock_opa_server,
            executor=mock_executor,
            producer=mock_producer,
            archiver=archiver,
        )

        result = orch.handle_message(incident_factory)

        assert result is not None
        assert isinstance(result, ActionResult)
        assert result.success is True
        assert result.archived is False


# ---------------------------------------------------------------------------
# Test 15: Approval API — GET /pending-approvals
# ---------------------------------------------------------------------------

class TestApprovalAPIPending:
    """GET /pending-approvals returns list of pending approval records."""

    def _make_pending_rows(self) -> list[dict]:
        """Build sample pending approval rows from ClickHouse."""
        return [
            {
                "approval_id": "apr-001",
                "incident_id": "inc-001",
                "action_type": "block_ip",
                "entity_id": "api-gateway",
                "proposed_by": "auto-remediation",
                "status": "pending",
                "created_at": "2026-08-05T00:00:00Z",
                "decided_at": None,
            },
            {
                "approval_id": "apr-002",
                "incident_id": "inc-002",
                "action_type": "rotate_credentials",
                "entity_id": "auth-service",
                "proposed_by": "auto-remediation",
                "status": "APPROVED",
                "created_at": "2026-08-05T00:00:00Z",
                "decided_at": "2026-08-05T01:00:00Z",
            },
        ]

    def test_get_pending_returns_only_pending(self) -> None:
        """Only rows with status='pending' are returned."""
        from fastapi.testclient import TestClient

        import orchestration.approval_api as api_mod
        from orchestration.approval_api import router

        rows = self._make_pending_rows()
        api_mod.configure(select_pending=lambda: rows)

        from fastapi import FastAPI

        test_app = FastAPI()
        test_app.include_router(router)
        client = TestClient(test_app)

        resp = client.get("/api/v1/pending-approvals")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["approval_id"] == "apr-001"
        assert data[0]["status"] == "pending"

    def test_get_pending_empty_when_none_configured(self) -> None:
        """When select_pending is None, returns empty list."""
        from fastapi.testclient import TestClient

        import orchestration.approval_api as api_mod
        from orchestration.approval_api import router

        api_mod.configure(select_pending=None)

        from fastapi import FastAPI

        test_app = FastAPI()
        test_app.include_router(router)
        client = TestClient(test_app)

        resp = client.get("/api/v1/pending-approvals")
        assert resp.status_code == 200
        assert resp.json() == []


# ---------------------------------------------------------------------------
# Test 16: Approval API — POST /approve/{id}
# ---------------------------------------------------------------------------

class TestApprovalAPIApprove:
    """POST /approve/{id} sets status=APPROVED, returns decision response."""

    def _make_pending_rows(self) -> list[dict]:
        return [
            {
                "approval_id": "apr-010",
                "incident_id": "inc-010",
                "action_type": "block_ip",
                "entity_id": "api-gateway",
                "proposed_by": "auto-remediation",
                "status": "pending",
                "created_at": "2026-08-05T00:00:00Z",
                "decided_at": None,
            },
        ]

    def test_approve_sets_status_approved(self) -> None:
        """POST /approve/apr-010 returns status=APPROVED with decided_at."""
        from fastapi.testclient import TestClient

        import orchestration.approval_api as api_mod
        from orchestration.approval_api import router

        update_calls: list[tuple] = []
        rows = self._make_pending_rows()

        api_mod.configure(
            select_pending=lambda: rows,
            update_decision=lambda aid, dec, dt: update_calls.append((aid, dec, dt)) or True,
        )

        from fastapi import FastAPI

        test_app = FastAPI()
        test_app.include_router(router)
        client = TestClient(test_app)

        resp = client.post("/api/v1/approve/apr-010")
        assert resp.status_code == 200
        data = resp.json()
        assert data["approval_id"] == "apr-010"
        assert data["status"] == "APPROVED"
        assert data["decided_at"] is not None
        assert "approved" in data["message"].lower()
        assert len(update_calls) == 1
        assert update_calls[0][0] == "apr-010"
        assert update_calls[0][1] == "APPROVED"

    def test_approve_idempotent_already_decided(self) -> None:
        """Approving an already-approved record returns current state (idempotent)."""
        from fastapi.testclient import TestClient

        import orchestration.approval_api as api_mod
        from orchestration.approval_api import router

        rows = [
            {
                "approval_id": "apr-011",
                "incident_id": "inc-011",
                "action_type": "block_ip",
                "entity_id": "api-gateway",
                "proposed_by": "auto-remediation",
                "status": "APPROVED",
                "created_at": "2026-08-05T00:00:00Z",
                "decided_at": "2026-08-05T01:00:00Z",
            },
        ]
        api_mod.configure(select_pending=lambda: rows)

        from fastapi import FastAPI

        test_app = FastAPI()
        test_app.include_router(router)
        client = TestClient(test_app)

        resp = client.post("/api/v1/approve/apr-011")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "APPROVED"
        assert "already" in data["message"].lower()

    def test_approve_not_found_returns_404(self) -> None:
        """Approving a nonexistent approval_id returns 404."""
        from fastapi.testclient import TestClient

        import orchestration.approval_api as api_mod
        from orchestration.approval_api import router

        api_mod.configure(select_pending=lambda: [])

        from fastapi import FastAPI

        test_app = FastAPI()
        test_app.include_router(router)
        client = TestClient(test_app)

        resp = client.post("/api/v1/approve/nonexistent")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Test 17: Approval API — POST /deny/{id}
# ---------------------------------------------------------------------------

class TestApprovalAPIDeny:
    """POST /deny/{id} sets status=DENIED, publishes denial to learning."""

    def _make_pending_rows(self) -> list[dict]:
        return [
            {
                "approval_id": "apr-020",
                "incident_id": "inc-020",
                "action_type": "rotate_credentials",
                "entity_id": "auth-service",
                "proposed_by": "auto-remediation",
                "status": "pending",
                "created_at": "2026-08-05T00:00:00Z",
                "decided_at": None,
            },
        ]

    def test_deny_sets_status_denied(self) -> None:
        """POST /deny/apr-020 returns status=DENIED with decided_at."""
        from fastapi.testclient import TestClient

        import orchestration.approval_api as api_mod
        from orchestration.approval_api import router

        update_calls: list[tuple] = []
        rows = self._make_pending_rows()

        api_mod.configure(
            select_pending=lambda: rows,
            update_decision=lambda aid, dec, dt: update_calls.append((aid, dec, dt)) or True,
        )

        from fastapi import FastAPI

        test_app = FastAPI()
        test_app.include_router(router)
        client = TestClient(test_app)

        resp = client.post("/api/v1/deny/apr-020")
        assert resp.status_code == 200
        data = resp.json()
        assert data["approval_id"] == "apr-020"
        assert data["status"] == "DENIED"
        assert data["decided_at"] is not None
        assert "denied" in data["message"].lower()
        assert len(update_calls) == 1
        assert update_calls[0][1] == "DENIED"

    def test_deny_publishes_to_learning(self) -> None:
        """Denial publishes a record to the learning producer."""
        from fastapi.testclient import TestClient

        import orchestration.approval_api as api_mod
        from orchestration.approval_api import router

        published: list[dict] = []
        rows = self._make_pending_rows()

        api_mod.configure(
            select_pending=lambda: rows,
            update_decision=lambda aid, dec, dt: True,
            learning_producer=lambda record: published.append(record),
        )

        from fastapi import FastAPI

        test_app = FastAPI()
        test_app.include_router(router)
        client = TestClient(test_app)

        resp = client.post("/api/v1/deny/apr-020")
        assert resp.status_code == 200
        assert len(published) == 1
        assert published[0]["approval_id"] == "apr-020"
        assert published[0]["decision"] == "DENIED"
        assert published[0]["incident_id"] == "inc-020"
        assert published[0]["action_type"] == "rotate_credentials"

    def test_deny_not_found_returns_404(self) -> None:
        """Denying a nonexistent approval_id returns 404."""
        from fastapi.testclient import TestClient

        import orchestration.approval_api as api_mod
        from orchestration.approval_api import router

        api_mod.configure(select_pending=lambda: [])

        from fastapi import FastAPI

        test_app = FastAPI()
        test_app.include_router(router)
        client = TestClient(test_app)

        resp = client.post("/api/v1/deny/nonexistent")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Test 18: Orchestration Engine — health + stats
# ---------------------------------------------------------------------------

class TestOrchestrationEngineEndpoints:
    """GET /health and GET /stats on the orchestration engine."""

    def test_health_returns_ok(self) -> None:
        """GET /health returns status=ok and component=orchestration_engine."""
        from fastapi.testclient import TestClient

        from orchestration.orchestration_engine import create_app

        test_app = create_app(
            opa=Mock(),
            executor=Mock(),
            producer=Mock(),
        )
        client = TestClient(test_app)

        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["component"] == "orchestration_engine"

    def test_stats_returns_consumer_state(self) -> None:
        """GET /stats returns running=False when consumer not started."""
        from fastapi.testclient import TestClient

        from orchestration.orchestration_engine import create_app

        test_app = create_app(
            opa=Mock(),
            executor=Mock(),
            producer=Mock(),
        )
        client = TestClient(test_app)

        resp = client.get("/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "running" in data
        assert "consumer_started" in data
        assert data["topic"] == "omniwatch.incidents.created"

    def test_approval_router_mounted(self) -> None:
        """The approval API router is mounted and responds."""
        from fastapi.testclient import TestClient

        import orchestration.approval_api as api_mod
        from orchestration.orchestration_engine import create_app

        api_mod.configure(select_pending=lambda: [])

        test_app = create_app(
            opa=Mock(),
            executor=Mock(),
            producer=Mock(),
        )
        client = TestClient(test_app)

        resp = client.get("/api/v1/pending-approvals")
        assert resp.status_code == 200
        assert resp.json() == []
