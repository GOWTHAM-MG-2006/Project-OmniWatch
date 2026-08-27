"""
OmniWatch — Dashboard API Tests
Component: Dashboard API Unit Tests
Phase: 11 — Dashboard + Continuous Learning
Purpose: Unit tests for the dashboard API endpoints with mocked backends.
Inputs: None (test module).
Outputs: Test results via pytest.
"""

from __future__ import annotations

# Patch heavy import-time deps before importing the app
import sys
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

_mock_cc = MagicMock()
_mock_neo4j = MagicMock()
_mock_minio = MagicMock()

# Ensure modules exist so import does not fail
sys.modules.setdefault("clickhouse_connect", _mock_cc)
sys.modules.setdefault("neo4j", _mock_neo4j)
sys.modules.setdefault("minio", _mock_minio)

from dashboard.api.main import create_app


@pytest.fixture()
def client() -> TestClient:
    """Create a fresh TestClient per test."""
    app = create_app()
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture()
def mock_ch() -> MagicMock:
    """Set up a mock ClickHouse client."""
    mock = MagicMock()
    mock.query.return_value = MagicMock(
        column_names=[("cnt",)],
        result_rows=[(42,)],
    )
    return mock


@pytest.fixture()
def mock_neo4j_driver() -> MagicMock:
    """Set up a mock Neo4j driver with session context."""
    session_mock = MagicMock()
    session_mock.run.return_value = [MagicMock(id="svc-1", name="order-service", labels=["Service"])]
    session_mock.__enter__ = MagicMock(return_value=session_mock)
    session_mock.__exit__ = MagicMock(return_value=False)

    driver_mock = MagicMock()
    driver_mock.session.return_value = session_mock
    return driver_mock


@pytest.fixture()
def mock_minio_client() -> MagicMock:
    """Set up a mock MinIO client."""
    mock = MagicMock()
    mock.list_objects.return_value = [MagicMock(object_name="audit-001.json")]
    mock.bucket_exists.return_value = True
    return mock


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class TestHealth:
    def test_health_returns_200(self, client: TestClient) -> None:
        resp = client.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert body["service"] == "dashboard-api"
        assert "timestamp" in body


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

class TestSummary:
    def test_summary_returns_counts(self, client: TestClient, mock_ch: MagicMock) -> None:
        # Mock three sequential queries returning different row counts
        mock_ch.query.side_effect = [
            MagicMock(column_names=[("cnt",)], result_rows=[(10,)]),  # incidents
            MagicMock(column_names=[("cnt",)], result_rows=[(5,)]),   # anomalies
            MagicMock(column_names=[("cnt",)], result_rows=[(20,)]),  # kb
        ]
        with patch("dashboard.api.main._get_ch_client", return_value=mock_ch):
            resp = client.get("/api/summary")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total_incidents"] == 10
        assert body["active_anomalies"] == 5
        assert body["knowledge_base_entries"] == 20

    def test_summary_graceful_on_ch_failure(self, client: TestClient) -> None:
        with patch("dashboard.api.main._get_ch_client", side_effect=Exception("CH down")):
            resp = client.get("/api/summary")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total_incidents"] == 0


# ---------------------------------------------------------------------------
# Incidents
# ---------------------------------------------------------------------------

class TestIncidents:
    def test_list_incidents(self, client: TestClient, mock_ch: MagicMock) -> None:
        mock_ch.query.return_value = MagicMock(
            column_names=[("incident_id", "severity")],
            result_rows=[("inc-1", "P1"), ("inc-2", "P2")],
        )
        with patch("dashboard.api.main._get_ch_client", return_value=mock_ch):
            resp = client.get("/api/incidents")
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 2

    def test_incident_detail_found(self, client: TestClient, mock_ch: MagicMock) -> None:
        mock_ch.query.return_value = MagicMock(
            column_names=[("incident_id",)],
            result_rows=[("inc-42",)],
        )
        with patch("dashboard.api.main._get_ch_client", return_value=mock_ch):
            resp = client.get("/api/incidents/inc-42")
        assert resp.status_code == 200
        assert resp.json()["incident"]["incident_id"] == "inc-42"

    def test_incident_detail_not_found(self, client: TestClient) -> None:
        mock = MagicMock()
        mock.query.return_value = MagicMock(column_names=[("incident_id",)], result_rows=[])
        with patch("dashboard.api.main._get_ch_client", return_value=mock):
            resp = client.get("/api/incidents/nonexistent")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Anomalies
# ---------------------------------------------------------------------------

class TestAnomalies:
    def test_list_anomalies(self, client: TestClient, mock_ch: MagicMock) -> None:
        mock_ch.query.return_value = MagicMock(
            column_names=[("anomaly_id",)],
            result_rows=[("a-1",), ("a-2",)],
        )
        with patch("dashboard.api.main._get_ch_client", return_value=mock_ch):
            resp = client.get("/api/anomalies")
        assert resp.status_code == 200
        assert resp.json()["count"] == 2

    def test_anomalies_with_entity_filter(self, client: TestClient, mock_ch: MagicMock) -> None:
        mock_ch.query.return_value = MagicMock(
            column_names=[("anomaly_id",)],
            result_rows=[("a-1",)],
        )
        with patch("dashboard.api.main._get_ch_client", return_value=mock_ch):
            resp = client.get("/api/anomalies?entity_id=svc-1&status=active")
        assert resp.status_code == 200
        assert resp.json()["count"] == 1


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

class TestMetrics:
    def test_list_metrics(self, client: TestClient, mock_ch: MagicMock) -> None:
        mock_ch.query.return_value = MagicMock(
            column_names=[("metric_name", "value")],
            result_rows=[("cpu_usage", 87.5)],
        )
        with patch("dashboard.api.main._get_ch_client", return_value=mock_ch):
            resp = client.get("/api/metrics")
        assert resp.status_code == 200
        assert resp.json()["count"] == 1

    def test_metrics_timeseries(self, client: TestClient, mock_ch: MagicMock) -> None:
        mock_ch.query.return_value = MagicMock(
            column_names=[("hour", "avg_value")],
            result_rows=[("2026-01-01T00:00:00", 85.0)],
        )
        with patch("dashboard.api.main._get_ch_client", return_value=mock_ch):
            resp = client.get("/api/metrics/timeseries?entity_id=svc-1&metric_name=cpu_usage&hours=24")
        assert resp.status_code == 200
        assert resp.json()["count"] == 1


# ---------------------------------------------------------------------------
# Logs
# ---------------------------------------------------------------------------

class TestLogs:
    def test_list_logs(self, client: TestClient, mock_ch: MagicMock) -> None:
        mock_ch.query.return_value = MagicMock(
            column_names=[("entity_id", "log_level", "message")],
            result_rows=[("svc-1", "ERROR", "connection refused")],
        )
        with patch("dashboard.api.main._get_ch_client", return_value=mock_ch):
            resp = client.get("/api/logs")
        assert resp.status_code == 200
        assert resp.json()["count"] == 1


# ---------------------------------------------------------------------------
# Traces
# ---------------------------------------------------------------------------

class TestTraces:
    def test_list_traces(self, client: TestClient, mock_ch: MagicMock) -> None:
        mock_ch.query.return_value = MagicMock(
            column_names=[("trace_id", "service_name", "duration_ms")],
            result_rows=[("t-1", "order-service", 125.0)],
        )
        with patch("dashboard.api.main._get_ch_client", return_value=mock_ch):
            resp = client.get("/api/traces")
        assert resp.status_code == 200
        assert resp.json()["count"] == 1


# ---------------------------------------------------------------------------
# Topology
# ---------------------------------------------------------------------------

class TestTopology:
    def test_topology_returns_flow_format(self, client: TestClient, mock_neo4j_driver: MagicMock) -> None:
        session_mock = MagicMock()
        session_mock.run.side_effect = [
            [{"id": "svc-1", "label": "order-service", "labels": ["Service"], "entity_type": "API_NODE", "criticality": "high", "status": "healthy", "anomaly_score": 0.1}],
            [{"source": "svc-1", "target": "db-1", "label": "CALLS", "latency_p50": 10.0, "error_rate": 0.01}],
        ]
        session_mock.__enter__ = MagicMock(return_value=session_mock)
        session_mock.__exit__ = MagicMock(return_value=False)
        mock_neo4j_driver.session.return_value = session_mock

        with patch("dashboard.api.main._get_neo4j_driver", return_value=mock_neo4j_driver):
            resp = client.get("/api/topology")
        assert resp.status_code == 200
        body = resp.json()
        assert body["node_count"] == 1
        assert body["edge_count"] == 1
        assert body["nodes"][0]["id"] == "svc-1"
        assert "position" in body["nodes"][0]


# ---------------------------------------------------------------------------
# Entities
# ---------------------------------------------------------------------------

class TestEntities:
    def test_list_entities(self, client: TestClient, mock_neo4j_driver: MagicMock) -> None:
        session_mock = MagicMock()
        session_mock.run.return_value = [MagicMock(id="svc-1", name="order-service")]
        session_mock.__enter__ = MagicMock(return_value=session_mock)
        session_mock.__exit__ = MagicMock(return_value=False)
        mock_neo4j_driver.session.return_value = session_mock

        with patch("dashboard.api.main._get_neo4j_driver", return_value=mock_neo4j_driver):
            resp = client.get("/api/entities")
        assert resp.status_code == 200
        assert resp.json()["count"] >= 1

    def test_entity_detail_not_found(self, client: TestClient, mock_neo4j_driver: MagicMock) -> None:
        session_mock = MagicMock()
        session_mock.run.return_value = []
        session_mock.__enter__ = MagicMock(return_value=session_mock)
        session_mock.__exit__ = MagicMock(return_value=False)
        mock_neo4j_driver.session.return_value = session_mock

        with patch("dashboard.api.main._get_neo4j_driver", return_value=mock_neo4j_driver):
            resp = client.get("/api/entity/nonexistent")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Knowledge Base
# ---------------------------------------------------------------------------

class TestKnowledgeBase:
    def test_list_knowledge_base(self, client: TestClient, mock_ch: MagicMock) -> None:
        mock_ch.query.return_value = MagicMock(
            column_names=[("kb_id",)],
            result_rows=[("kb-1",)],
        )
        with patch("dashboard.api.main._get_ch_client", return_value=mock_ch):
            resp = client.get("/api/knowledge-base")
        assert resp.status_code == 200
        assert resp.json()["count"] == 1


# ---------------------------------------------------------------------------
# Audit Logs (MinIO)
# ---------------------------------------------------------------------------

class TestAuditLogs:
    def test_list_audit_logs(self, client: TestClient, mock_minio_client: MagicMock) -> None:
        with patch("dashboard.api.main._get_minio_client", return_value=mock_minio_client):
            resp = client.get("/api/audit-logs")
        assert resp.status_code == 200
        assert resp.json()["count"] == 1

    def test_audit_log_detail_not_found(self, client: TestClient) -> None:
        mock = MagicMock()
        mock.get_object.side_effect = Exception("not found")
        with patch("dashboard.api.main._get_minio_client", return_value=mock):
            resp = client.get("/api/audit-logs/nonexistent.json")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Copilot
# ---------------------------------------------------------------------------

class TestCopilot:
    def test_copilot_unavailable(self, client: TestClient) -> None:
        with patch("httpx.AsyncClient") as mock_httpx:
            mock_instance = MagicMock()
            mock_instance.post.side_effect = Exception("connection refused")
            mock_instance.__aenter__ = MagicMock(return_value=mock_instance)
            mock_instance.__aexit__ = MagicMock(return_value=False)
            mock_httpx.return_value = mock_instance

            resp = client.get("/api/copilot?question=what+is+healthy")
        assert resp.status_code == 200
        body = resp.json()
        assert "unavailable" in body["answer"].lower() or "error" in body


# ---------------------------------------------------------------------------
# Storage Health
# ---------------------------------------------------------------------------

class TestStorageHealth:
    def test_storage_health_all_ok(self, client: TestClient, mock_ch: MagicMock, mock_neo4j_driver: MagicMock, mock_minio_client: MagicMock) -> None:
        mock_ch.query.return_value = MagicMock(column_names=[("1",)], result_rows=[(1,)])
        session_mock = MagicMock()
        session_mock.run.return_value = [MagicMock()]
        session_mock.__enter__ = MagicMock(return_value=session_mock)
        session_mock.__exit__ = MagicMock(return_value=False)
        mock_neo4j_driver.session.return_value = session_mock

        with (
            patch("dashboard.api.main._get_ch_client", return_value=mock_ch),
            patch("dashboard.api.main._get_neo4j_driver", return_value=mock_neo4j_driver),
            patch("dashboard.api.main._get_minio_client", return_value=mock_minio_client),
        ):
            resp = client.get("/api/storage-health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["clickhouse"] is True
        assert body["neo4j"] is True
        assert body["minio"] is True
        assert body["all_healthy"] is True


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

class TestStats:
    def test_stats_returns_table_counts(self, client: TestClient, mock_ch: MagicMock) -> None:
        mock_ch.query.return_value = MagicMock(
            column_names=[("cnt",)],
            result_rows=[(100,)],
        )
        with patch("dashboard.api.main._get_ch_client", return_value=mock_ch):
            resp = client.get("/api/stats")
        assert resp.status_code == 200
        body = resp.json()
        assert "stats" in body
        assert len(body["stats"]) == 7  # 7 tables


# ---------------------------------------------------------------------------
# Pending Approvals
# ---------------------------------------------------------------------------

class TestPendingApprovals:
    def test_list_pending_approvals(self, client: TestClient, mock_ch: MagicMock) -> None:
        mock_ch.query.return_value = MagicMock(
            column_names=[("approval_id",)],
            result_rows=[("ap-1",)],
        )
        with patch("dashboard.api.main._get_ch_client", return_value=mock_ch):
            resp = client.get("/api/pending-approvals")
        assert resp.status_code == 200
        assert resp.json()["count"] == 1
