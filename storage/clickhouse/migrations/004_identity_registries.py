"""
OmniWatch — Storage Layer
Component: ClickHouse Migration 004 — Identity Registry Tables
Phase: entry-point (exporter-importer-split)
Purpose: Idempotent DDL migration creating the registry tables that let the
         identity service survive restarts: workspace_importers (one row per
         workspace, token hashes only), workspace_exporters (append-only with
         deleted flag), agent_bindings (append-only with deleted flag).
         Workspaces/users themselves live in migration 002/003 tables.
         identity/persistence.py mirrors every mutation here (best-effort)
         and reloads all registries from here on boot (latest row per key
         wins; deleted rows are skipped).
Inputs: CLICKHOUSE_HOST/PORT/DB/USER/PASSWORD env vars (via
        StorageConfig.from_env(), defaults match docker-compose.yml)
Outputs: `workspace_importers`, `workspace_exporters`, `agent_bindings`
         in the `omniwatch` database.

NOTE: ClickHouse has no UNIQUE constraint — per-key latest-wins is enforced
at reload time in identity/persistence.py (ORDER BY created_at DESC
LIMIT 1 BY <key>). Rows are never purged here; deletes are tombstones
(deleted=1), mirroring the workspaces-table convention from 003.

Run:
    python -m storage.clickhouse.migrations.004_identity_registries
"""

from __future__ import annotations

import sys
from typing import Any, List

from storage.common import create_logger, retry_with_backoff
from storage.config import StorageConfig

# Retry contract shared with migrations 001/002/003: 3 retries after the
# initial attempt, sleeping 100ms -> 500ms -> 2s between attempts.
_RETRIES = 3
_BASE_DELAY = 0.1
_MAX_DELAY = 2.0

STATEMENTS: List[str] = [
    """CREATE TABLE IF NOT EXISTS workspace_importers (
  workspace_id String,
  user_id String,
  slug String,
  endpoint_url String,
  token_hash String,
  created_at DateTime,
  mirrored_at DateTime DEFAULT created_at
) ENGINE = MergeTree() ORDER BY workspace_id""",
    """CREATE TABLE IF NOT EXISTS workspace_exporters (
  workspace_id String,
  user_id String,
  number UInt32,
  name String,
  created_at DateTime,
  deleted UInt8,
  mirrored_at DateTime DEFAULT created_at
) ENGINE = MergeTree() ORDER BY (workspace_id, number)""",
    """CREATE TABLE IF NOT EXISTS agent_bindings (
  binding_id String,
  user_id String,
  workspace_id String,
  workspace_slug String,
  agent_endpoint String,
  token_hash String,
  status String,
  created_at DateTime,
  deleted UInt8,
  mirrored_at DateTime DEFAULT created_at
) ENGINE = MergeTree() ORDER BY binding_id""",
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
    """Apply the identity registry tables; return executed statements.

    Connection and each statement execution are wrapped in
    retry_with_backoff (3x, 100ms -> 500ms -> 2s). Safe to call
    repeatedly — DDL is IF NOT EXISTS throughout.
    """
    log = logger or create_logger("omniwatch.storage.clickhouse.migrations.004")

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
        "migration 004 applied %d statements (idempotent)", len(STATEMENTS))
    return list(STATEMENTS)


def main() -> int:
    """CLI entry point: apply the identity registry tables to ClickHouse."""
    log = create_logger("omniwatch.storage.clickhouse.migrations.004")
    try:
        statements = apply_schema(logger=log)
    except Exception as exc:  # noqa: BLE001 - CLI reports the root cause
        log.error("migration 004 failed: %s", exc)
        return 1
    log.info("migration 004 applied %d statements", len(statements))
    return 0


if __name__ == "__main__":
    sys.exit(main())
