"""
OmniWatch — Storage Layer
Component: ClickHouse Migration 003 — Workspaces Table
Phase: entry-point (Wave 2, todo 2)
Purpose: Idempotent DDL migration creating the `workspaces` table consumed
         by the workspace registry mirror (identity/provision.py).
         CREATE TABLE IF NOT EXISTS throughout => safe to run repeatedly.
Inputs: CLICKHOUSE_HOST/PORT/DB/USER/PASSWORD env vars (via
        StorageConfig.from_env(), defaults match docker-compose.yml)
Outputs: `workspaces(workspace_id UUID, user_id UUID, name String,
         slug String, app_type String, cloud_provider String,
         endpoints Array(String), expected_volume String,
         retention_days UInt32, created_at DateTime, deleted UInt8)`
         in the `omniwatch` database.

NOTE: ClickHouse has no UNIQUE constraint — per-user name uniqueness and
global slug uniqueness are enforced by the service layer
(store-side checks in identity/workspaces.py, DuplicateWorkspaceNameError
-> 409). Delete is a tombstone (deleted=1); rows are never purged here.

Run:
    python -m storage.clickhouse.migrations.003_workspaces
"""

from __future__ import annotations

import sys
from typing import Any, List

from storage.common import create_logger, retry_with_backoff
from storage.config import StorageConfig

# Retry contract shared with migrations 001/002: 3 retries after the initial
# attempt, sleeping 100ms -> 500ms -> 2s between attempts.
_RETRIES = 3
_BASE_DELAY = 0.1
_MAX_DELAY = 2.0

STATEMENTS: List[str] = [
    """CREATE TABLE IF NOT EXISTS workspaces (
  workspace_id UUID,
  user_id UUID,
  name String,
  slug String,
  app_type String,
  cloud_provider String,
  endpoints Array(String),
  expected_volume String,
  retention_days UInt32,
  created_at DateTime,
  deleted UInt8
) ENGINE = MergeTree() ORDER BY workspace_id""",
]


def _connect(cfg: StorageConfig) -> Any:
    """Open a clickhouse-connect client for the configured ClickHouse."""
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=cfg.clickhouse_host,
        port=cfg.clickhouse_port,
        database=cfg.clickhouse_db,
        username=cfg.clickhouse_user,
        password=cfg.clickhouse_password,
    )


def _execute(client: Any, statement: str) -> None:
    """Execute one DDL statement against the ClickHouse client."""
    client.command(statement)


def apply_schema(logger: Any | None = None) -> List[str]:
    """Apply the workspaces table to ClickHouse; return executed statements.

    Connection and each statement execution are wrapped in
    retry_with_backoff (3x, 100ms -> 500ms -> 2s). Safe to call
    repeatedly — DDL is IF NOT EXISTS throughout.
    """
    log = logger or create_logger("omniwatch.storage.clickhouse.migrations.003")

    cfg = StorageConfig.from_env()
    client = retry_with_backoff(
        _connect,
        retries=_RETRIES,
        base_delay=_BASE_DELAY,
        max_delay=_MAX_DELAY,
        logger=log,
        cfg=cfg,
    )
    try:
        for statement in STATEMENTS:
            retry_with_backoff(
                _execute,
                retries=_RETRIES,
                base_delay=_BASE_DELAY,
                max_delay=_MAX_DELAY,
                logger=log,
                client=client,
                statement=statement,
            )
            log.info("executed: %.80s", statement)
    finally:
        client.close()

    log.info(
        "migration 003 applied %d statements (idempotent)", len(STATEMENTS))
    return list(STATEMENTS)


def main() -> int:
    """CLI entry point: apply the workspaces table to ClickHouse."""
    log = create_logger("omniwatch.storage.clickhouse.migrations.003")
    try:
        statements = apply_schema(logger=log)
    except Exception as exc:  # noqa: BLE001 - CLI reports the root cause
        log.error("migration 003 failed: %s", exc)
        return 1
    log.info("migration 003 applied %d statements", len(statements))
    return 0


if __name__ == "__main__":
    sys.exit(main())
