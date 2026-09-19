"""
OmniWatch — Storage Layer
Component: ClickHouse Migration 003a — Onboarding Columns on Workspaces
Phase: entry-point (Wave 2, todo 3)
Purpose: Idempotent ALTER migration adding the wizard answer columns consumed
         by the onboarding row mirror (identity/onboarding.py).
         ALTER ... ADD COLUMN IF NOT EXISTS throughout => safe to run
         repeatedly, safe to run before or after 003.
Inputs: CLICKHOUSE_HOST/PORT/DB/USER/PASSWORD env vars (via
        StorageConfig.from_env(), defaults match docker-compose.yml)
Outputs: `workspaces` gains onboarding_app_name, onboarding_app_type,
         onboarding_cloud_provider, onboarding_endpoints,
         onboarding_expected_eps, onboarding_log_volume,
         onboarding_retention_days, onboarding_alert_contact,
         onboarding_config_json, onboarding_updated_at.

DECISION RECORD (ENTRY-3): new columns on the existing `workspaces` table
(rather than a separate onboarding table) because GET must return the same
payload as POST with a single-row read, the wizard is 1:1 with a workspace,
and resubmit-updates map to a column overwrite. The identity service mirrors
best-effort (memory store skips; live CH inserts the full row including these
columns). MinIO workspaces/<slug>/config.json stays the durable artifact.

Run:
    python -m storage.clickhouse.migrations.003a_onboarding
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
    "ALTER TABLE IF EXISTS workspaces ADD COLUMN IF NOT EXISTS "
    "onboarding_app_name String DEFAULT ''",
    "ALTER TABLE IF EXISTS workspaces ADD COLUMN IF NOT EXISTS "
    "onboarding_app_type String DEFAULT ''",
    "ALTER TABLE IF EXISTS workspaces ADD COLUMN IF NOT EXISTS "
    "onboarding_cloud_provider String DEFAULT ''",
    "ALTER TABLE IF EXISTS workspaces ADD COLUMN IF NOT EXISTS "
    "onboarding_endpoints Array(String) DEFAULT []",
    "ALTER TABLE IF EXISTS workspaces ADD COLUMN IF NOT EXISTS "
    "onboarding_expected_eps String DEFAULT ''",
    "ALTER TABLE IF EXISTS workspaces ADD COLUMN IF NOT EXISTS "
    "onboarding_log_volume String DEFAULT ''",
    "ALTER TABLE IF EXISTS workspaces ADD COLUMN IF NOT EXISTS "
    "onboarding_retention_days UInt32 DEFAULT 0",
    "ALTER TABLE IF EXISTS workspaces ADD COLUMN IF NOT EXISTS "
    "onboarding_alert_contact String DEFAULT ''",
    "ALTER TABLE IF EXISTS workspaces ADD COLUMN IF NOT EXISTS "
    "onboarding_config_json String DEFAULT ''",
    "ALTER TABLE IF EXISTS workspaces ADD COLUMN IF NOT EXISTS "
    "onboarding_updated_at DateTime DEFAULT now()",
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
    """Execute one ALTER statement against the ClickHouse client."""
    client.command(statement)


def apply_schema(logger: Any | None = None) -> List[str]:
    """Apply the onboarding columns to ClickHouse; return executed statements.

    Connection and each statement execution are wrapped in
    retry_with_backoff (3x, 100ms -> 500ms -> 2s). Safe to call
    repeatedly — ALTERs are IF EXISTS / IF NOT EXISTS throughout.
    """
    log = logger or create_logger("omniwatch.storage.clickhouse.migrations.003a")

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
        "migration 003a applied %d statements (idempotent)", len(STATEMENTS))
    return list(STATEMENTS)


def main() -> int:
    """CLI entry point: apply the onboarding columns to ClickHouse."""
    log = create_logger("omniwatch.storage.clickhouse.migrations.003a")
    try:
        statements = apply_schema(logger=log)
    except Exception as exc:  # noqa: BLE001 - CLI reports the root cause
        log.error("migration 003a failed: %s", exc)
        return 1
    log.info("migration 003a applied %d statements", len(statements))
    return 0


if __name__ == "__main__":
    sys.exit(main())
