"""
OmniWatch — Unified Storage Layer
Component: Aggregated Health Endpoint
Phase: 5
Purpose: Aggregated health summary for the Unified Storage Layer — runs each
         backend's health_check() and returns a single {clickhouse, neo4j,
         minio} result map consumed by the dashboard/API gateway. One down
         store is reported as False (never crashes the check); every client
         opened is always closed, even on partial failure.
Inputs: StorageConfig (via StorageConfig.from_env(), or passed explicitly)
Outputs: {"clickhouse": bool, "neo4j": bool, "minio": bool,
          "all_healthy": bool} health summary dict; pretty JSON to stdout
          when run via `python -m storage.health` (exit 0 all healthy, else 1)
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, Optional

from storage.clickhouse.client import ClickHouseClient
from storage.common import create_logger
from storage.config import StorageConfig
from storage.minio.client import MinioClient
from storage.neo4j.client import Neo4jClient

logger = create_logger("omniwatch.storage.health")


def check_storage_health(config: Optional[StorageConfig] = None) -> Dict[str, Any]:
    """Run ``health_check()`` against all three storage backends.

    Builds a fresh ClickHouseClient, Neo4jClient, and MinioClient from ONE
    ``StorageConfig`` (``StorageConfig.from_env()`` when ``config`` is None),
    probes each store, and always ``close()``s every client it opened — even
    when a store's check fails — so no connections leak on partial failure.

    Per-store failures are contained: a down store sets its flag to False and
    logs an error, and the remaining stores are still probed, so a single
    outage cannot crash the whole check.
    """
    cfg = config or StorageConfig.from_env()
    clickhouse = ClickHouseClient(cfg)
    neo4j = Neo4jClient(cfg)
    minio = MinioClient(cfg)

    results: Dict[str, bool] = {}
    try:
        for store, client in (
            ("clickhouse", clickhouse),
            ("neo4j", neo4j),
            ("minio", minio),
        ):
            try:
                results[store] = bool(client.health_check())
            except Exception as exc:  # noqa: BLE001 - one down store must not kill the check
                logger.error("%s health check failed: %s", store, exc)
                results[store] = False
    finally:
        # Release every client we opened. MinioClient wraps the SDK pool with
        # no close() of its own, so call close() only where it exists.
        for client in (clickhouse, neo4j, minio):
            close = getattr(client, "close", None)
            if callable(close):
                close()

    results["all_healthy"] = all(
        results.get(k, False) for k in ("clickhouse", "neo4j", "minio")
    )
    return results


if __name__ == "__main__":
    summary = check_storage_health()
    print(json.dumps(summary, indent=2))
    sys.exit(0 if summary["all_healthy"] else 1)
