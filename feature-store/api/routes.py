"""
OmniWatch — Feature Store (Windowing Layer)
Component: REST API Routes
Phase: 4
Purpose: Feature retrieval endpoints — serve windowed feature vectors from the
         ClickHouse ``feature_vectors`` table to downstream consumers
         (Phase 6 anomaly detection).
Inputs: GET /features/{entity_id}?window_size=5m&start=<ISO 8601>&end=<ISO 8601>
Outputs: Feature vectors as JSON; 404 empty-result / 503 ClickHouse-down errors
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

try:
    from .clickhouse_client import ClickHouseClient, ClickHouseUnavailable
except ImportError:  # pragma: no cover - python main.py direct-run mode
    from clickhouse_client import ClickHouseClient, ClickHouseUnavailable


class FeatureVector(BaseModel):
    """One row of the 15-column ``feature_vectors`` table."""

    entity_id: str
    window_start: datetime
    window_end: datetime
    window_size: str
    latency_p50: float
    latency_p95: float
    latency_p99: float
    latency_avg: float
    latency_min: float
    latency_max: float
    error_rate: float
    request_volume: int
    feature_version: int
    ttl: int
    timestamp: datetime


logger = logging.getLogger("omniwatch.feature_store.api")

# Shared client instance — the application's ClickHouse connection pool.
_client = ClickHouseClient()

router = APIRouter(tags=["features"])


def _parse_iso(value: str) -> datetime:
    """Parse an ISO 8601 string to a naive UTC datetime (ClickHouse DateTime)."""
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"invalid ISO 8601 timestamp: {value!r}") from exc
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


@router.get(
    "/features/{entity_id}",
    response_model=List[FeatureVector],
    summary="Get feature vectors for an entity",
    description="Windowed feature vectors from the feature_vectors table, "
    "optionally filtered by window_size and an ISO 8601 start/end range "
    "(default: last 24h).",
    responses={
        400: {"description": "Invalid window_size or start/end timestamps"},
        404: {"description": "No feature vectors found for the entity"},
        503: {"description": "ClickHouse unavailable"},
    },
)
def get_features(
    entity_id: str,
    window_size: Optional[str] = Query(
        default=None, description="Window size filter, e.g. 5m, 15m, 1h"
    ),
    start: Optional[str] = Query(
        default=None, description="ISO 8601 start of range (default: now - 24h)"
    ),
    end: Optional[str] = Query(
        default=None, description="ISO 8601 end of range (default: now)"
    ),
) -> Any:
    try:
        start_dt = _parse_iso(start) if start else None
        end_dt = _parse_iso(end) if end else None
        rows = _client.query_features(
            entity_id=entity_id,
            window_size=window_size,
            start=start_dt,
            end=end_dt,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ClickHouseUnavailable as exc:
        logger.error("features request failed for entity=%s: %s", entity_id, exc)
        raise HTTPException(
            status_code=503, detail="ClickHouse unavailable"
        ) from exc

    if not rows:
        raise HTTPException(
            status_code=404,
            detail={
                "message": f"no feature vectors found for entity {entity_id!r}",
                "features": [],
            },
        )
    return [FeatureVector.model_validate(row) for row in rows]
