"""
OmniWatch — Continuous Learning Layer
Component: Feedback Loop Tests
Phase: 11
Purpose: Unit tests for FeedbackLoopProcessor — mock Kafka + ClickHouse,
         verify correct knowledge_base inserts and column handling.
"""

from __future__ import annotations

import json
import uuid
from unittest.mock import MagicMock

import pytest

from learning.feedback_loop import KB_COLUMNS, FeedbackLoopProcessor


@pytest.fixture
def sample_action() -> dict:
    return {
        "action_id": str(uuid.uuid4()),
        "incident_id": "inc-001",
        "action_type": "restart_service",
        "entity_id": "postgresql-database",
        "entity_type": "DATABASE_NODE",
        "success": True,
        "output": "simulated: restart_service on postgresql-database",
        "error": None,
        "execution_time_seconds": 0.001,
        "executed_at": "2026-08-27T12:00:00+00:00",
        "triggered_by": "auto",
        "dry_run": False,
        "needs_approval": False,
        "approval_id": None,
        "severity": "P1",
        "confidence": 92.0,
        "archived": False,
    }


@pytest.fixture
def failed_action() -> dict:
    return {
        "action_id": str(uuid.uuid4()),
        "incident_id": "inc-002",
        "action_type": "rollback",
        "entity_id": "api-gateway",
        "entity_type": "API_NODE",
        "success": False,
        "output": "",
        "error": "rollback failed: timeout",
        "execution_time_seconds": 5.0,
        "executed_at": "2026-08-27T12:05:00+00:00",
        "triggered_by": "auto",
        "severity": "P2",
        "confidence": 85.0,
        "archived": False,
    }


@pytest.fixture
def mock_ch_client():
    client = MagicMock()
    client.insert = MagicMock(return_value=1)
    client.command = MagicMock()
    return client


@pytest.fixture
def processor(mock_ch_client):
    proc = FeedbackLoopProcessor(
        clickhouse_config={"host": "localhost", "port": 8123}
    )
    proc._ch_client = mock_ch_client
    return proc


class TestBuildKbRow:
    def test_maps_action_fields(self, processor, sample_action):
        row = processor._build_kb_row(sample_action)

        assert row["incident_id"] == "inc-001"
        assert row["root_cause_entity"] == "postgresql-database"
        assert row["root_cause_entity_type"] == "DATABASE_NODE"
        assert row["resolution_summary"] == "simulated: restart_service on postgresql-database"
        assert row["outcome"] == "success"
        assert row["action_type"] == "restart_service"
        assert row["success_count"] == 1

    def test_failed_action_maps_zero_success(self, processor, failed_action):
        row = processor._build_kb_row(failed_action)

        assert row["outcome"] == "failure"
        assert row["success_count"] == 0
        assert row["actions_taken"] == json.dumps(["rollback"])

    def test_kb_id_is_uuid(self, processor, sample_action):
        row = processor._build_kb_row(sample_action)
        uuid.UUID(row["kb_id"])

    def test_created_at_is_iso_string(self, processor, sample_action):
        row = processor._build_kb_row(sample_action)
        assert "T" in row["created_at"]


class TestNormalizeKbRow:
    def test_column_count_matches(self):
        row = {col: f"val_{col}" for col in KB_COLUMNS}
        normalized = FeedbackLoopProcessor._normalize_kb_row(row)
        assert len(normalized) == len(KB_COLUMNS)

    def test_created_at_parsed(self):
        row = {"created_at": "2026-08-27T12:00:00+00:00"}
        normalized = FeedbackLoopProcessor._normalize_kb_row(row)
        from datetime import datetime

        assert isinstance(normalized[KB_COLUMNS.index("created_at")], datetime)


class TestProcessMessage:
    def test_success_action_inserts_row(self, processor, sample_action, mock_ch_client):
        raw = json.dumps(sample_action).encode()
        result = processor.process_message(raw)

        assert result is True
        mock_ch_client.insert.assert_called_once()
        call_args = mock_ch_client.insert.call_args
        assert call_args[0][0] == "omniwatch.knowledge_base"

    def test_failed_action_inserts_row(self, processor, failed_action, mock_ch_client):
        raw = json.dumps(failed_action).encode()
        result = processor.process_message(raw)

        assert result is True
        mock_ch_client.insert.assert_called_once()

    def test_invalid_json_returns_false(self, processor, mock_ch_client):
        result = processor.process_message(b"not-json")
        assert result is False

    def test_insert_failure_returns_false(self, processor, sample_action, mock_ch_client):
        mock_ch_client.insert.side_effect = Exception("connection refused")
        raw = json.dumps(sample_action).encode()
        result = processor.process_message(raw)

        assert result is False


class TestEnsureColumns:
    def test_ensure_columns_calls_alter(self, processor, mock_ch_client):
        processor._ensure_columns()
        assert mock_ch_client.command.call_count == 2
        calls = [c[0][0] for c in mock_ch_client.command.call_args_list]
        assert any("action_type" in c for c in calls)
        assert any("success_count" in c for c in calls)
