"""
OmniWatch — Storage Layer
Component: ClickHouse Migration 002 — Identity Tables
Phase: entry-point (Wave 1)
Purpose: Idempotent DDL migration creating the `users` and `sessions`
         tables consumed by the identity service (identity/store.py).
         CREATE TABLE IF NOT EXISTS throughout => safe to run repeatedly.
Inputs: CLICKHOUSE_HOST/PORT/DB/USER/PASSWORD env vars (via
        StorageConfig.from_env(), defaults match docker-compose.yml)
Outputs: `users(user_id UUID, email String, pw_hash String,
         created_at DateTime)` and `sessions(session_id UUID, user_id UUID,
         refresh_hash String, expires_at DateTime, revoked UInt8)` in the
         `omniwatch` database.

NOTE: ClickHouse has no UNIQUE constraint — email uniqueness is enforced
by the service layer (SELECT-before-INSERT in store.ClickHouseStore,
DuplicateEmailError -> 409).

Run:
    python -m storage.clickhouse.migrations.002_identity
"""

from __future__ import annotations

import sys
from typing import Any, List

from storage.common import create_logger, retry_with_backoff
from storage.config import StorageConfig

# Retry contract shared with migration 001: 3 retries after the initial
# attempt, sleeping 100ms -> 500ms -> 2s between attempts.
_RETRIES = 3
_BASE_DELAY = 0.1
_MAX_DELAY = 2.0

STATEMENTS: List[str] = [
    """CREATE TABLE IF NOT EXISTS users (
  user_id UUID,
  email String,
  pw_hash String,
  created_at DateTime
) ENGINE = MergeTree() ORDER BY user_id""",
    """CREATE TABLE IF NOT EXISTS sessions (
  session_id UUID,
  user_id UUID,
  refresh_hash String,
  expires_at DateTime,
  revoked UInt8
) ENGINE = MergeTree() ORDER BY session_id""",
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
    """Apply the identity tables to ClickHouse; return executed statements.

    Connection and each statement execution are wrapped in
    retry_with_backoff (3x, 100ms -> 500ms -> 2s). Safe to call
    repeatedly — DDL is IF NOT EXISTS throughout.
    """
    log = logger or create_logger("omniwatch.storage.clickhouse.migrations.002")

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
        "migration 002 applied %d statements (idempotent)", len(STATEMENTS))
    return list(STATEMENTS)


def main() -> int:
    """CLI entry point: apply the identity tables to ClickHouse."""
    log = create_logger("omniwatch.storage.clickhouse.migrations.002")
    try:
        statements = apply_schema(logger=log)
    except Exception as exc:  # noqa: BLE001 - CLI reports the root cause
        log.error("migration 002 failed: %s", exc)
        return 1
    log.info("migration 002 applied %d statements", len(statements))
    return 0


if __name__ == "__main__":
    sys.exit(main())
