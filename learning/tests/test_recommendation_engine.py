"""
OmniWatch — Continuous Learning Layer
Component: Recommendation Engine Tests
Phase: 11
Purpose: Unit tests for RecommendationEngine, verifying query shape,
         confidence calculation, and edge-case handling.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

from learning.recommendation_engine import RecommendationEngine

# ------------------------------------------------------------------ #
# Helpers
# ------------------------------------------------------------------ #

def _make_result_rows(rows: list[tuple[Any, Any]]) -> MagicMock:
    """Build a mock clickhouse_connect query result."""
    mock_result = MagicMock()
    mock_result.result_rows = rows
    return mock_result


def _make_engine(mock_client: MagicMock | None = None) -> RecommendationEngine:
    """Construct a RecommendationEngine with a pre-injected mock client."""
    engine = RecommendationEngine()
    if mock_client is not None:
        engine._ch_client = mock_client
    return engine


# ------------------------------------------------------------------ #
# Tests
# ------------------------------------------------------------------ #


class TestGetRecommendations:
    """Tests for RecommendationEngine.get_recommendations()."""

    def test_returns_top3_by_success_count(self) -> None:
        """Verify query returns top-3 results sorted by success count."""
        # SQL LIMIT 3 means ClickHouse returns at most 3 rows
        mock_client = MagicMock()
        mock_client.query.return_value = _make_result_rows([
            ("restart_pod", 10),
            ("scale_deployment", 8),
            ("clear_cache", 5),
        ])
        engine = _make_engine(mock_client)

        recs = engine.get_recommendations("postgresql-database")

        assert len(recs) == 3
        assert recs[0]["action_type"] == "restart_pod"
        assert recs[0]["success_count"] == 10
        assert recs[1]["action_type"] == "scale_deployment"
        assert recs[2]["action_type"] == "clear_cache"

    def test_confidence_calculation_correct(self) -> None:
        """Verify confidence = success_count / total * 100."""
        mock_client = MagicMock()
        mock_client.query.return_value = _make_result_rows([
            ("restart_pod", 10),
            ("scale_deployment", 5),
            ("clear_cache", 5),
        ])
        engine = _make_engine(mock_client)

        recs = engine.get_recommendations("order-service")

        # total = 20, restart_pod = 10/20*100 = 50.0
        assert recs[0]["confidence"] == 50.0
        # scale_deployment = 5/20*100 = 25.0
        assert recs[1]["confidence"] == 25.0
        assert recs[2]["confidence"] == 25.0

    def test_filters_empty_action_type(self) -> None:
        """Verify rows with empty or None action_type are filtered out."""
        mock_client = MagicMock()
        mock_client.query.return_value = _make_result_rows([
            ("restart_pod", 10),
            ("", 5),
            (None, 3),
            ("scale_deployment", 8),
        ])
        engine = _make_engine(mock_client)

        recs = engine.get_recommendations("postgresql-database")

        # Only restart_pod and scale_deployment should remain
        assert len(recs) == 2
        assert recs[0]["action_type"] == "restart_pod"
        assert recs[1]["action_type"] == "scale_deployment"

    def test_returns_empty_on_no_results(self) -> None:
        """Verify empty list returned when query returns no rows."""
        mock_client = MagicMock()
        mock_client.query.return_value = _make_result_rows([])
        engine = _make_engine(mock_client)

        recs = engine.get_recommendations("unknown-entity")

        assert recs == []

    def test_returns_empty_on_query_failure(self) -> None:
        """Verify exception during query is caught and [] returned."""
        mock_client = MagicMock()
        mock_client.query.side_effect = Exception("connection refused")
        engine = _make_engine(mock_client)

        recs = engine.get_recommendations("postgresql-database")

        assert recs == []

    def test_returns_empty_on_empty_entity_id(self) -> None:
        """Verify empty string entity_id returns [] without querying."""
        mock_client = MagicMock()
        engine = _make_engine(mock_client)

        recs = engine.get_recommendations("")

        assert recs == []
        mock_client.query.assert_not_called()

    def test_query_contains_group_by_and_order(self) -> None:
        """QA assertion: verify SQL contains required GROUP BY / ORDER BY."""
        mock_client = MagicMock()
        mock_client.query.return_value = _make_result_rows([
            ("restart_pod", 10),
        ])
        engine = _make_engine(mock_client)
        engine.get_recommendations("test-entity")

        call_args = mock_client.query.call_args
        sql = call_args[0][0] if call_args[0] else ""
        assert "GROUP BY action_type" in sql
        assert "ORDER BY total_success DESC" in sql
        assert "LIMIT 3" in sql

    def test_query_uses_parameterized_entity_id(self) -> None:
        """Verify entity_id is passed as a parameter, not interpolated."""
        mock_client = MagicMock()
        mock_client.query.return_value = _make_result_rows([
            ("restart_pod", 10),
        ])
        engine = _make_engine(mock_client)
        engine.get_recommendations("postgresql-database")

        call_kwargs = mock_client.query.call_args[1]
        assert "parameters" in call_kwargs
        assert call_kwargs["parameters"]["entity_id"] == "postgresql-database"

    def test_single_action_one_hundred_percent_confidence(self) -> None:
        """Verify single action gets 100% confidence."""
        mock_client = MagicMock()
        mock_client.query.return_value = _make_result_rows([
            ("restart_pod", 15),
        ])
        engine = _make_engine(mock_client)

        recs = engine.get_recommendations("single-entity")

        assert len(recs) == 1
        assert recs[0]["confidence"] == 100.0

    def test_returns_at_most_max_recommendations(self) -> None:
        """Verify engine respects max_recommendations limit."""
        # SQL LIMIT 2 means ClickHouse returns at most 2 rows
        mock_client = MagicMock()
        mock_client.query.return_value = _make_result_rows([
            ("a", 10),
            ("b", 8),
        ])
        engine = _make_engine(mock_client)
        engine._max_recommendations = 2

        recs = engine.get_recommendations("entity")

        assert len(recs) == 2


class TestClose:
    """Tests for RecommendationEngine.close()."""

    def test_close_sets_client_none(self) -> None:
        """Verify close() closes client and sets it to None."""
        mock_client = MagicMock()
        engine = _make_engine(mock_client)

        engine.close()

        mock_client.close.assert_called_once()
        assert engine._ch_client is None

    def test_close_idempotent(self) -> None:
        """Verify calling close() multiple times does not error."""
        engine = _make_engine(None)
        engine.close()
        engine.close()
        assert engine._ch_client is None


class TestGetChClient:
    """Tests for lazy ClickHouse client initialization."""

    @patch("learning.recommendation_engine.clickhouse_connect")
    def test_lazy_init_creates_client(self, mock_ch: MagicMock) -> None:
        """Verify _get_ch_client creates client on first call."""
        engine = RecommendationEngine()
        engine._get_ch_client()

        mock_ch.get_client.assert_called_once()
        assert engine._ch_client is not None

    @patch("learning.recommendation_engine.clickhouse_connect")
    def test_lazy_init_reuses_client(self, mock_ch: MagicMock) -> None:
        """Verify _get_ch_client reuses existing client."""
        engine = RecommendationEngine()
        engine._get_ch_client()
        engine._get_ch_client()

        mock_ch.get_client.assert_called_once()
