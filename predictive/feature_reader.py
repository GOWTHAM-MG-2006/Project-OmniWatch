"""
OmniWatch — Predictive Intelligence Layer
Component: Feature Reader
Phase: 6
Purpose: Read windowed feature vectors from ClickHouse for anomaly detection
Inputs: entity_id, optional window_size / start / end filters
Outputs: list[dict] of feature rows in ascending timestamp order (oldest→newest)
"""

from __future__ import annotations

import logging
from typing import Any

from predictive.config.settings import Settings
from storage.clickhouse.client import ClickHouseClient
from storage.config import StorageConfig

logger = logging.getLogger("omniwatch.predictive.feature_reader")


class FeatureReader:
    """Read windowed feature vectors from the ClickHouse ``feature_vectors``
    table (Phase 4 output) for use by the Phase 6 anomaly detector.

    The underlying ``ClickHouseClient.select_by_entity`` returns rows in
    **descending** timestamp order (newest first).  ``read_features`` reverses
    the result so the caller receives rows in **ascending** chronological
    order — oldest first — which is the training order expected by the
    detector.

    This component reads **only** from ClickHouse; it does NOT consume Kafka.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or Settings(_env_file=None)
        self._ch_client: ClickHouseClient | None = None

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def read_features(
        self,
        entity_id: str,
        window_size: str | None = None,
        start: str | None = None,
        end: str | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Return feature vectors for *entity_id*, oldest→newest.

        Parameters
        ----------
        entity_id:
            Entity to fetch features for (e.g. ``"order-service"``).
        window_size:
            Optional filter — only rows whose ``window_size`` column matches
            (e.g. ``"5m"``).
        start:
            Optional ISO-8601 lower bound (inclusive) for ``timestamp``.
        end:
            Optional ISO-8601 upper bound (inclusive) for ``timestamp``.
        limit:
            Maximum rows to request from ClickHouse (default 1000).

        Returns
        -------
        list[dict]
            Feature row dicts in **ascending** timestamp order.  Returns an
            empty list on any error (table missing, connection failure, etc.).
        """
        try:
            client = self._get_ch_client()
            rows = client.select_by_entity(
                entity_id,
                table="feature_vectors",
                limit=limit,
            )

            # select_by_entity returns DESC (newest first) — reverse to
            # ascending chronological order for the detector.
            rows = list(reversed(rows))

            # Optional Python-side filters (avoids SQL injection surface).
            if window_size is not None:
                rows = [r for r in rows if r.get("window_size") == window_size]
            if start is not None:
                rows = [r for r in rows if r.get("timestamp", "") >= start]
            if end is not None:
                rows = [r for r in rows if r.get("timestamp", "") <= end]

            return rows

        except Exception as exc:  # noqa: BLE001 — graceful degradation
            logger.warning(
                "Failed to read feature vectors for entity_id=%s: %s",
                entity_id,
                exc,
            )
            return []

    def close(self) -> None:
        """Close the underlying ClickHouse client connection."""
        if self._ch_client is not None:
            self._ch_client.close()
            self._ch_client = None

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _get_ch_client(self) -> ClickHouseClient:
        """Lazily create a ``ClickHouseClient`` from ``Settings`` fields."""
        if self._ch_client is None:
            cfg = StorageConfig(
                clickhouse_host=self._settings.clickhouse_host,
                clickhouse_port=self._settings.clickhouse_port,
                clickhouse_db=self._settings.clickhouse_db,
                clickhouse_user=self._settings.clickhouse_user,
                clickhouse_password=self._settings.clickhouse_password,
            )
            self._ch_client = ClickHouseClient(config=cfg)
        return self._ch_client
