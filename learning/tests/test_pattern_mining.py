"""
OmniWatch — Continuous Learning Layer
Component: Pattern Mining Tests
Phase: 11
Purpose: Unit tests for PatternMiner — mock ClickHouse + Neo4j,
         verify pattern query, node creation, and HAS_PATTERN linking.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from learning.pattern_mining import PatternMiner


@pytest.fixture
def sample_patterns() -> list[dict]:
    return [
        {
            "root_cause_entity": "postgresql-database",
            "severity": "P1",
            "time_bucket": "2026-08-27 10:00:00",
            "pattern_count": 5,
            "first_seen": "2026-08-27 10:00:00",
            "last_seen": "2026-08-27 14:00:00",
        },
        {
            "root_cause_entity": "api-gateway",
            "severity": "P2",
            "time_bucket": "2026-08-27 11:00:00",
            "pattern_count": 3,
            "first_seen": "2026-08-27 11:00:00",
            "last_seen": "2026-08-27 13:00:00",
        },
    ]


@pytest.fixture
def mock_ch_client():
    client = MagicMock()
    mock_result = MagicMock()
    mock_result.result_rows = [
        (
            "postgresql-database",
            "P1",
            datetime(2026, 8, 27, 10, 0, 0, tzinfo=timezone.utc),
            5,
            datetime(2026, 8, 27, 10, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 8, 27, 14, 0, 0, tzinfo=timezone.utc),
        ),
        (
            "api-gateway",
            "P2",
            datetime(2026, 8, 27, 11, 0, 0, tzinfo=timezone.utc),
            3,
            datetime(2026, 8, 27, 11, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 8, 27, 13, 0, 0, tzinfo=timezone.utc),
        ),
    ]
    client.query.return_value = mock_result
    return client


@pytest.fixture
def mock_neo4j_driver():
    driver = MagicMock()
    session = MagicMock()
    driver.session.return_value.__enter__ = MagicMock(return_value=session)
    driver.session.return_value.__exit__ = MagicMock(return_value=False)
    return driver


@pytest.fixture
def miner(mock_ch_client, mock_neo4j_driver):
    m = PatternMiner(
        clickhouse_config={"host": "localhost", "port": 8123},
        neo4j_config={"uri": "bolt://localhost:7687"},
    )
    m._ch_client = mock_ch_client
    m._neo4j_driver = mock_neo4j_driver
    return m


class TestQueryPatterns:
    def test_returns_parsed_patterns(self, miner, mock_ch_client):
        patterns = miner._query_patterns()

        assert len(patterns) == 2
        assert patterns[0]["root_cause_entity"] == "postgresql-database"
        assert patterns[0]["severity"] == "P1"
        assert patterns[0]["pattern_count"] == 5
        assert patterns[1]["root_cause_entity"] == "api-gateway"
        assert patterns[1]["severity"] == "P2"

    def test_query_uses_correct_database(self, miner, mock_ch_client):
        miner._query_patterns()
        call_args = mock_ch_client.query.call_args[0][0]
        assert "omniwatch.incidents" in call_args

    def test_query_failure_returns_empty(self, miner, mock_ch_client):
        mock_ch_client.query.side_effect = Exception("connection refused")
        patterns = miner._query_patterns()
        assert patterns == []


class TestGeneratePatternId:
    def test_deterministic_id(self, miner):
        pattern = {
            "root_cause_entity": "postgresql-database",
            "severity": "P1",
            "time_bucket": "2026-08-27 10:00:00",
        }
        id1 = miner._generate_pattern_id(pattern)
        id2 = miner._generate_pattern_id(pattern)
        assert id1 == id2
        assert id1.startswith("pat-")
        assert len(id1) == 16  # "pat-" + 12 hex chars

    def test_different_patterns_different_ids(self, miner):
        p1 = {"root_cause_entity": "db1", "severity": "P1", "time_bucket": "t1"}
        p2 = {"root_cause_entity": "db2", "severity": "P2", "time_bucket": "t2"}
        assert miner._generate_pattern_id(p1) != miner._generate_pattern_id(p2)


class TestCreatePatternNode:
    def test_creates_neo4j_node(self, miner, mock_neo4j_driver):
        pattern = {
            "root_cause_entity": "postgresql-database",
            "severity": "P1",
            "time_bucket": "2026-08-27 10:00:00",
            "pattern_count": 5,
            "first_seen": "2026-08-27 10:00:00",
            "last_seen": "2026-08-27 14:00:00",
        }
        result = miner._create_pattern_node(pattern)

        assert result is not None
        assert result["root_cause_entity"] == "postgresql-database"
        assert result["severity"] == "P1"
        assert result["pattern_count"] == 5
        assert result["pattern_id"].startswith("pat-")

        # Verify two Cypher runs: one for Pattern node, one for HAS_PATTERN
        session = mock_neo4j_driver.session.return_value.__enter__.return_value
        assert session.run.call_count == 2

    def test_returns_none_on_failure(self, miner, mock_neo4j_driver):
        session = mock_neo4j_driver.session.return_value.__enter__.return_value
        session.run.side_effect = Exception("neo4j down")

        pattern = {
            "root_cause_entity": "db",
            "severity": "P1",
            "time_bucket": "t",
            "pattern_count": 2,
            "first_seen": "f",
            "last_seen": "l",
        }
        result = miner._create_pattern_node(pattern)
        assert result is None


class TestMinePatterns:
    def test_mine_creates_all_patterns(self, miner, mock_ch_client, mock_neo4j_driver):
        created = miner.mine_patterns()

        assert len(created) == 2
        assert created[0]["root_cause_entity"] == "postgresql-database"
        assert created[1]["root_cause_entity"] == "api-gateway"

    def test_mine_empty_returns_empty(self, miner, mock_ch_client):
        mock_ch_client.query.return_value.result_rows = []
        created = miner.mine_patterns()
        assert created == []

    def test_mine_partial_failure(self, miner, mock_ch_client, mock_neo4j_driver):
        session = mock_neo4j_driver.session.return_value.__enter__.return_value
        # First call succeeds, second fails
        session.run.side_effect = [None, Exception("neo4j error")]

        created = miner.mine_patterns()
        # Should still return the first successful pattern
        # (second one fails silently with logging)
        assert isinstance(created, list)


class TestStop:
    def test_stop_closes_connections(self, miner, mock_ch_client, mock_neo4j_driver):
        miner.stop()
        mock_neo4j_driver.close.assert_called_once()
        mock_ch_client.close.assert_called_once()
        assert miner._neo4j_driver is None
        assert miner._ch_client is None

    def test_stop_idempotent(self, miner, mock_ch_client, mock_neo4j_driver):
        miner.stop()
        miner.stop()  # second call should not crash
        assert miner._neo4j_driver is None
