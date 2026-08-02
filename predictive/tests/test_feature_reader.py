"""
OmniWatch — Predictive Intelligence Layer
Component: FeatureReader unit tests
Phase: 6
Purpose: Verify FeatureReader reads feature vectors from ClickHouse in ascending order
Inputs: None
Outputs: pytest pass/fail
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from predictive.feature_reader import FeatureReader
from predictive.config.settings import Settings


def _make_settings() -> Settings:
    return Settings(_env_file=None)


def _sample_rows_desc() -> list[dict[str, Any]]:
    """Three feature-vector rows in DESCENDING timestamp order (newest first),
    matching what ClickHouseClient.select_by_entity returns."""
    return [
        {
            "entity_id": "order-service",
            "window_start": "2026-08-02T12:10:00",
            "window_end": "2026-08-02T12:15:00",
            "window_size": "5m",
            "latency_p50": 12.0,
            "latency_p95": 45.0,
            "latency_p99": 80.0,
            "latency_avg": 18.5,
            "latency_min": 5.0,
            "latency_max": 95.0,
            "error_rate": 0.02,
            "request_volume": 500,
            "feature_version": 1,
            "ttl": 86400,
            "timestamp": "2026-08-02T12:15:00",
        },
        {
            "entity_id": "order-service",
            "window_start": "2026-08-02T12:05:00",
            "window_end": "2026-08-02T12:10:00",
            "window_size": "5m",
            "latency_p50": 11.0,
            "latency_p95": 40.0,
            "latency_p99": 75.0,
            "latency_avg": 17.0,
            "latency_min": 4.0,
            "latency_max": 88.0,
            "error_rate": 0.01,
            "request_volume": 450,
            "feature_version": 1,
            "ttl": 86400,
            "timestamp": "2026-08-02T12:10:00",
        },
        {
            "entity_id": "order-service",
            "window_start": "2026-08-02T12:00:00",
            "window_end": "2026-08-02T12:05:00",
            "window_size": "5m",
            "latency_p50": 10.0,
            "latency_p95": 35.0,
            "latency_p99": 70.0,
            "latency_avg": 15.0,
            "latency_min": 3.0,
            "latency_max": 82.0,
            "error_rate": 0.005,
            "request_volume": 400,
            "feature_version": 1,
            "ttl": 86400,
            "timestamp": "2026-08-02T12:05:00",
        },
    ]


def _row(
    entity_id: str = "order-service",
    timestamp: str = "2026-08-02T12:00:00",
    window_size: str = "5m",
) -> dict[str, Any]:
    """Minimal feature-vector row."""
    return {
        "entity_id": entity_id,
        "window_start": timestamp,
        "window_end": timestamp,
        "window_size": window_size,
        "latency_p50": 10.0,
        "latency_p95": 35.0,
        "latency_p99": 70.0,
        "latency_avg": 15.0,
        "latency_min": 3.0,
        "latency_max": 82.0,
        "error_rate": 0.005,
        "request_volume": 400,
        "feature_version": 1,
        "ttl": 86400,
        "timestamp": timestamp,
    }


# ------------------------------------------------------------------- #
# Tests — read_features()
# ------------------------------------------------------------------- #


class TestReadFeatures:

    @patch("predictive.feature_reader.ClickHouseClient")
    def test_returns_rows_ascending_timestamp(
        self,
        mock_ch_cls: MagicMock,
    ) -> None:
        """select_by_entity returns DESC — read_features must reverse to ASC."""
        mock_ch_cls.return_value.select_by_entity.return_value = _sample_rows_desc()

        reader = FeatureReader(settings=_make_settings())
        rows = reader.read_features("order-service")

        assert len(rows) == 3
        timestamps = [r["timestamp"] for r in rows]
        assert timestamps == sorted(timestamps), (
            "Rows must be in ascending timestamp order (oldest first)"
        )
        assert timestamps == [
            "2026-08-02T12:05:00",
            "2026-08-02T12:10:00",
            "2026-08-02T12:15:00",
        ]
        reader.close()

    @patch("predictive.feature_reader.ClickHouseClient")
    def test_empty_result_returns_empty_list(
        self,
        mock_ch_cls: MagicMock,
    ) -> None:
        mock_ch_cls.return_value.select_by_entity.return_value = []

        reader = FeatureReader(settings=_make_settings())
        rows = reader.read_features("nonexistent-entity")

        assert rows == []
        reader.close()

    @patch("predictive.feature_reader.ClickHouseClient")
    def test_window_size_filter(
        self,
        mock_ch_cls: MagicMock,
    ) -> None:
        """Only rows matching window_size are returned."""
        mixed = [
            _row(window_size="1m", timestamp="2026-08-02T12:00:00"),
            _row(window_size="5m", timestamp="2026-08-02T12:01:00"),
            _row(window_size="5m", timestamp="2026-08-02T12:02:00"),
            _row(window_size="15m", timestamp="2026-08-02T12:03:00"),
        ]
        mock_ch_cls.return_value.select_by_entity.return_value = list(reversed(mixed))

        reader = FeatureReader(settings=_make_settings())
        rows = reader.read_features("order-service", window_size="5m")

        assert len(rows) == 2
        assert all(r["window_size"] == "5m" for r in rows)
        reader.close()

    @patch("predictive.feature_reader.ClickHouseClient")
    def test_start_end_filter(
        self,
        mock_ch_cls: MagicMock,
    ) -> None:
        """Only rows within [start, end] are returned."""
        rows_desc = [
            _row(timestamp="2026-08-02T14:00:00"),
            _row(timestamp="2026-08-02T12:30:00"),
            _row(timestamp="2026-08-02T11:00:00"),
            _row(timestamp="2026-08-02T10:00:00"),
        ]
        mock_ch_cls.return_value.select_by_entity.return_value = rows_desc

        reader = FeatureReader(settings=_make_settings())
        rows = reader.read_features(
            "order-service",
            start="2026-08-02T11:00:00",
            end="2026-08-02T13:00:00",
        )

        timestamps = [r["timestamp"] for r in rows]
        assert "2026-08-02T10:00:00" not in timestamps
        assert "2026-08-02T14:00:00" not in timestamps
        assert timestamps == ["2026-08-02T11:00:00", "2026-08-02T12:30:00"]
        reader.close()

    @patch("predictive.feature_reader.ClickHouseClient")
    def test_start_only_filter(
        self,
        mock_ch_cls: MagicMock,
    ) -> None:
        """Only start is provided — rows before start excluded."""
        mock_ch_cls.return_value.select_by_entity.return_value = [
            _row(timestamp="2026-08-02T15:00:00"),
            _row(timestamp="2026-08-02T12:00:00"),
            _row(timestamp="2026-08-02T09:00:00"),
        ]

        reader = FeatureReader(settings=_make_settings())
        rows = reader.read_features("order-service", start="2026-08-02T12:00:00")

        timestamps = [r["timestamp"] for r in rows]
        assert timestamps == ["2026-08-02T12:00:00", "2026-08-02T15:00:00"]
        reader.close()

    @patch("predictive.feature_reader.ClickHouseClient")
    def test_exception_returns_empty_list(
        self,
        mock_ch_cls: MagicMock,
    ) -> None:
        """Table missing / connection failure → graceful [] without raising."""
        mock_ch_cls.return_value.select_by_entity.side_effect = RuntimeError(
            "Table feature_vectors doesn't exist"
        )

        reader = FeatureReader(settings=_make_settings())
        rows = reader.read_features("order-service")

        assert rows == []
        reader.close()

    @patch("predictive.feature_reader.ClickHouseClient")
    def test_passes_correct_limit(
        self,
        mock_ch_cls: MagicMock,
    ) -> None:
        """Custom limit is forwarded to select_by_entity."""
        mock_ch_cls.return_value.select_by_entity.return_value = []

        reader = FeatureReader(settings=_make_settings())
        reader.read_features("order-service", limit=50)

        mock_ch_cls.return_value.select_by_entity.assert_called_once_with(
            "order-service",
            table="feature_vectors",
            limit=50,
        )
        reader.close()


# ------------------------------------------------------------------- #
# Tests — close()
# ------------------------------------------------------------------- #


class TestClose:

    @patch("predictive.feature_reader.ClickHouseClient")
    def test_close_calls_client_close(
        self,
        mock_ch_cls: MagicMock,
    ) -> None:
        reader = FeatureReader(settings=_make_settings())
        # Force client creation
        reader._get_ch_client()
        reader.close()

        mock_ch_cls.return_value.close.assert_called_once()

    @patch("predictive.feature_reader.ClickHouseClient")
    def test_close_without_prior_use(
        self,
        mock_ch_cls: MagicMock,
    ) -> None:
        """close() on a reader that never queried should be a no-op."""
        reader = FeatureReader(settings=_make_settings())
        reader.close()

        mock_ch_cls.return_value.close.assert_not_called()

    @patch("predictive.feature_reader.ClickHouseClient")
    def test_close_sets_client_to_none(
        self,
        mock_ch_cls: MagicMock,
    ) -> None:
        reader = FeatureReader(settings=_make_settings())
        reader._get_ch_client()
        reader.close()

        assert reader._ch_client is None

    @patch("predictive.feature_reader.ClickHouseClient")
    def test_close_allows_reconnection(
        self,
        mock_ch_cls: MagicMock,
    ) -> None:
        """After close(), next read creates a fresh client."""
        mock_ch_cls.return_value.select_by_entity.return_value = []

        reader = FeatureReader(settings=_make_settings())
        reader._get_ch_client()
        reader.close()

        # Next read should create a new client
        reader.read_features("order-service")

        assert mock_ch_cls.call_count == 2  # __init__ called twice


# ------------------------------------------------------------------- #
# Tests — lazy client creation
# ------------------------------------------------------------------- #


class TestLazyClient:

    @patch("predictive.feature_reader.ClickHouseClient")
    def test_client_not_created_before_read(
        self,
        mock_ch_cls: MagicMock,
    ) -> None:
        reader = FeatureReader(settings=_make_settings())

        assert reader._ch_client is None
        mock_ch_cls.assert_not_called()
        reader.close()

    @patch("predictive.feature_reader.ClickHouseClient")
    def test_client_created_on_first_read(
        self,
        mock_ch_cls: MagicMock,
    ) -> None:
        mock_ch_cls.return_value.select_by_entity.return_value = []

        reader = FeatureReader(settings=_make_settings())
        reader.read_features("order-service")

        mock_ch_cls.assert_called_once()
        reader.close()
