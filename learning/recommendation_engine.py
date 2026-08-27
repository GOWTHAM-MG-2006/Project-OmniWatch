"""
OmniWatch — Continuous Learning Layer
Component: Recommendation Engine
Phase: 11
Purpose: Query ClickHouse knowledge_base for historically successful actions
         matching a given root_cause_entity, returning top-3 recommendations
         with confidence scores.
Inputs: ClickHouse knowledge_base table (action_type, success_count,
        root_cause_entity, outcome)
Outputs: Top-3 recommendations as list of dicts with action_type,
         success_count, and confidence fields
"""

from __future__ import annotations

import logging
import os
from typing import Any

import clickhouse_connect

logger = logging.getLogger("omniwatch.learning.recommendation_engine")

# ClickHouse connection settings (env vars match feedback_loop.py pattern)
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "omniwatch")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")

# Maximum recommendations to return
MAX_RECOMMENDATIONS = 3

# Query: top actions by cumulative success_count for a given entity
_RECOMMENDATION_QUERY = """
SELECT action_type, SUM(success_count) as total_success
FROM {database}.knowledge_base
WHERE root_cause_entity = %(entity_id)s
  AND outcome = 'success'
GROUP BY action_type
ORDER BY total_success DESC
LIMIT {limit}
"""


class RecommendationEngine:
    """Query ClickHouse knowledge_base for historically successful remediation actions.

    Provides top-N recommendations with confidence scores for a given
    root_cause_entity.  The engine is designed for lazy initialization —
    the ClickHouse connection is established on the first query, not at
    construction time.

    Lifecycle:
        1. ``__init__`` — store config, defer client creation.
        2. ``get_recommendations(entity_id)`` — query and score.
        3. ``close()`` — release ClickHouse connection.
    """

    def __init__(
        self,
        clickhouse_config: dict[str, Any] | None = None,
        max_recommendations: int = MAX_RECOMMENDATIONS,
    ) -> None:
        self._max_recommendations = max_recommendations

        # ClickHouse connection (lazy init).
        ch_overrides = clickhouse_config or {}
        self._ch_host = ch_overrides.get("host", CLICKHOUSE_HOST)
        self._ch_port = int(ch_overrides.get("port", CLICKHOUSE_PORT))
        self._ch_db = ch_overrides.get("database", CLICKHOUSE_DB)
        self._ch_user = ch_overrides.get("username", CLICKHOUSE_USER)
        self._ch_password = ch_overrides.get("password", CLICKHOUSE_PASSWORD)
        self._ch_client: Any = None

    # ------------------------------------------------------------------ #
    # Connection helpers
    # ------------------------------------------------------------------ #

    def _get_ch_client(self) -> Any:
        """Return the ClickHouse client, creating it lazily on first call."""
        if self._ch_client is None:
            self._ch_client = clickhouse_connect.get_client(
                host=self._ch_host,
                port=self._ch_port,
                database=self._ch_db,
                username=self._ch_user,
                password=self._ch_password,
            )
            logger.info(
                "clickhouse_connected host=%s port=%d db=%s",
                self._ch_host,
                self._ch_port,
                self._ch_db,
            )
        return self._ch_client

    # ------------------------------------------------------------------ #
    # Core recommendation logic
    # ------------------------------------------------------------------ #

    def get_recommendations(self, entity_id: str) -> list[dict[str, Any]]:
        """Return top-N historically successful actions for *entity_id*.

        Each recommendation contains:
            - ``action_type``: the remediation action (e.g. ``restart_pod``).
            - ``success_count``: cumulative success count from knowledge_base.
            - ``confidence``: percentage of total successes this action represents.

        Returns an empty list when no successful actions exist or on query failure.
        """
        if not entity_id:
            logger.warning("get_recommendations called with empty entity_id")
            return []

        sql = _RECOMMENDATION_QUERY.format(
            database=self._ch_db,
            limit=self._max_recommendations,
        )

        try:
            client = self._get_ch_client()
            result = client.query(
                sql,
                parameters={"entity_id": entity_id},
            )
            rows = result.result_rows
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "recommendation_query_failed entity_id=%s error=%s",
                entity_id,
                exc,
            )
            return []

        # Filter out empty action_type values
        filtered = [
            (row[0], int(row[1]))
            for row in rows
            if row[0] is not None and row[0] != ""
        ]

        if not filtered:
            logger.info("no_recommendations_found entity_id=%s", entity_id)
            return []

        # Compute confidence: row_success / total_success * 100
        total_success = sum(success for _, success in filtered)
        if total_success == 0:
            logger.info("all_success_counts_zero entity_id=%s", entity_id)
            return []

        recommendations: list[dict[str, Any]] = []
        for action_type, success_count in filtered:
            confidence = round(success_count / total_success * 100, 1)
            recommendations.append({
                "action_type": action_type,
                "success_count": success_count,
                "confidence": confidence,
            })

        logger.info(
            "recommendations_ready entity_id=%s count=%d",
            entity_id,
            len(recommendations),
        )
        return recommendations

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    def close(self) -> None:
        """Close the ClickHouse connection if open."""
        if self._ch_client is not None:
            try:
                self._ch_client.close()
            except Exception:  # noqa: BLE001, S110
                pass
            self._ch_client = None
