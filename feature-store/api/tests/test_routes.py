"""
OmniWatch — Feature Store (Windowing Layer)
Component: API Route Tests
Phase: 4
Purpose: Validate /health and /features/{entity_id} behaviour with ClickHouse
         fully mocked — tests must pass without a live ClickHouse instance.
Inputs: HTTP requests via FastAPI TestClient
Outputs: Pass/fail assertions
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import main
import routes
from clickhouse_client import ClickHouseUnavailable
from fastapi.testclient import TestClient

client = TestClient(main.app)

EXPECTED_FEATURE_KEYS = {
    "entity_id",
    "window_start",
    "window_end",
    "window_size",
    "latency_p50",
    "latency_p95",
    "latency_p99",
    "latency_avg",
    "latency_min",
    "latency_max",
    "error_rate",
    "request_volume",
    "feature_version",
    "ttl",
    "timestamp",
}


def _sample_row(**overrides) -> dict:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    row = {
        "entity_id": "postgresql-database",
        "window_start": (now - timedelta(minutes=5)).isoformat(),
        "window_end": now.isoformat(),
        "window_size": "5m",
        "latency_p50": 12.5,
        "latency_p95": 30.1,
        "latency_p99": 45.0,
        "latency_avg": 15.2,
        "latency_min": 1.0,
        "latency_max": 60.0,
        "error_rate": 0.01,
        "request_volume": 1000,
        "feature_version": 1,
        "ttl": 90,
        "timestamp": now.isoformat(),
    }
    row.update(overrides)
    return row


def test_health() -> None:
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "healthy", "service": "feature-store-api"}


@patch.object(routes._client, "query_features", return_value=[_sample_row()])
def test_features_returns_feature_vector_json(mock_query) -> None:
    resp = client.get("/features/postgresql-database?window_size=5m")
    assert resp.status_code == 200
    payload = resp.json()
    assert isinstance(payload, list)
    assert len(payload) == 1
    assert set(payload[0].keys()) == EXPECTED_FEATURE_KEYS
    assert payload[0]["entity_id"] == "postgresql-database"
    assert payload[0]["window_size"] == "5m"


@patch.object(routes._client, "query_features", return_value=[])
def test_features_empty_returns_404(mock_query) -> None:
    resp = client.get("/features/unknown-entity")
    assert resp.status_code == 404
    body = resp.json()
    assert body["detail"]["features"] == []
    assert "no feature vectors" in body["detail"]["message"]


@patch.object(
    routes._client, "query_features", side_effect=ClickHouseUnavailable("down")
)
def test_features_clickhouse_down_returns_503(mock_query) -> None:
    resp = client.get("/features/postgresql-database")
    assert resp.status_code == 503
    assert resp.json()["detail"] == "ClickHouse unavailable"


@patch.object(routes._client, "query_features")
def test_features_invalid_start_returns_400(mock_query) -> None:
    resp = client.get("/features/postgresql-database?start=not-a-date")
    assert resp.status_code == 400
    mock_query.assert_not_called()


@patch.object(routes._client, "query_features", return_value=[])
def test_features_passes_query_params(mock_query) -> None:
    client.get(
        "/features/entity-1?window_size=15m"
        "&start=2026-07-31T00:00:00Z&end=2026-07-31T12:00:00Z"
    )
    mock_query.assert_called_once()
    _, kwargs = mock_query.call_args
    assert kwargs["entity_id"] == "entity-1"
    assert kwargs["window_size"] == "15m"
    assert kwargs["start"] == datetime(2026, 7, 31, 0, 0, 0)
    assert kwargs["end"] == datetime(2026, 7, 31, 12, 0, 0)


@patch.object(routes._client, "query_features")
def test_features_defaults_to_last_24h(mock_query) -> None:
    mock_query.return_value = []
    client.get("/features/entity-1")
    _, kwargs = mock_query.call_args
    assert kwargs["start"] is None
    assert kwargs["end"] is None
    assert kwargs["window_size"] is None
