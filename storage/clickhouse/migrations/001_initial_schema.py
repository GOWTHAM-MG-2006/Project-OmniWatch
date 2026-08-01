"""
OmniWatch — Storage Layer
Component: ClickHouse Migration 001 — Initial Schema
Phase: 5
Purpose: Idempotent DDL migration — reads storage/clickhouse/schema.sql,
         splits it into statements on ';', and executes each against
         ClickHouse via clickhouse-connect (HTTP port 8123). Because
         schema.sql uses CREATE DATABASE/TABLE IF NOT EXISTS throughout,
         the migration is safe to run any number of times.
Inputs: storage/clickhouse/schema.sql (path relative to this file);
        CLICKHOUSE_HOST/PORT/DB/USER/PASSWORD env vars (via
        StorageConfig.from_env(), defaults match docker-compose.yml)
Outputs: ClickHouse database `omniwatch` with the 7 Phase 5 tables created
         (metrics, logs, traces, anomalies, incidents, pending_approvals,
         knowledge_base). Does NOT create feature_vectors (Phase 4 owns it).

Run:
    python -m storage.clickhouse.migrations.001_initial_schema
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, List

import clickhouse_connect

from storage.common import create_logger, retry_with_backoff
from storage.config import StorageConfig

# Path of this migration file's parent directory (the migrations/ package).
_MIGRATIONS_DIR = Path(__file__).resolve().parent
# schema.sql lives one level up, in storage/clickhouse/.
_SCHEMA_PATH = _MIGRATIONS_DIR.parent / "schema.sql"

# Retry contract shared with Phase 4 clickhouse_client.py: 3 retries after
# the initial attempt, sleeping 100ms -> 500ms -> 2s between attempts.
_RETRIES = 3
_BASE_DELAY = 0.1
_MAX_DELAY = 2.0


def _split_sql_statements(text: str) -> List[str]:
    """Split a SQL script into statements on TOP-LEVEL semicolons only.

    A naive ``text.split(';')`` is broken for schema.sql because its SQL
    comments embed semicolons — both full-line comments
    (``-- Partitioning: toYYYYMMDD(timestamp); TTL: 90 days ...``) and
    trailing column comments (``-- UTC event time; partition + TTL key``).
    Semicolons inside ``--`` line comments, ``/* */`` block comments, and
    single-quoted string literals are therefore ignored; only ``;`` outside
    those contexts terminates a statement. Comment text is kept (ClickHouse
    accepts leading/inline ``--`` comments) so the DDL is applied verbatim.
    Empty/whitespace-only fragments are dropped.
    """
    statements: List[str] = []
    current: List[str] = []
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        nxt = text[i + 1] if i + 1 < n else ""
        if ch == "-" and nxt == "-":  # -- line comment: consume to EOL
            while i < n and text[i] != "\n":
                current.append(text[i])
                i += 1
        elif ch == "/" and nxt == "*":  # /* block comment */
            current.append(ch)
            current.append(nxt)
            i += 2
            while i < n:
                current.append(text[i])
                if text[i] == "*" and i + 1 < n and text[i + 1] == "/":
                    current.append(text[i + 1])
                    i += 2
                    break
                i += 1
        elif ch == "'":  # single-quoted string literal
            current.append(ch)
            i += 1
            while i < n:
                current.append(text[i])
                i += 1
                if text[i - 1] == "'":
                    break
        elif ch == ";":  # top-level statement terminator
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
            i += 1
        else:
            current.append(ch)
            i += 1
    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


def load_schema_statements(schema_path: Path | None = None) -> List[str]:
    """Read schema.sql and return its individual SQL statements.

    Uses a comment/string-aware split (``_split_sql_statements``) so the
    semicolons embedded in schema.sql's SQL comments cannot corrupt the
    statement boundaries. The DDL statements are applied verbatim.
    """
    path = schema_path or _SCHEMA_PATH
    text = path.read_text(encoding="utf-8")
    return _split_sql_statements(text)


def _connect(cfg: StorageConfig) -> Any:
    """Open a clickhouse-connect client for the configured ClickHouse."""
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
    """Apply schema.sql to ClickHouse and return the executed statements.

    Connection and each statement execution are wrapped in
    retry_with_backoff (3x, 100ms -> 500ms -> 2s). Safe to call
    repeatedly — schema.sql is idempotent.
    """
    log = logger or create_logger("omniwatch.storage.clickhouse.migrations.001")

    cfg = StorageConfig.from_env()
    statements = load_schema_statements()
    log.info(
        "loaded %d schema statements from %s",
        len(statements),
        _SCHEMA_PATH,
    )

    client = retry_with_backoff(
        _connect,
        retries=_RETRIES,
        base_delay=_BASE_DELAY,
        max_delay=_MAX_DELAY,
        logger=log,
        cfg=cfg,
    )
    try:
        for statement in statements:
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
        "schema migration complete: %d statements applied (idempotent)",
        len(statements),
    )
    return statements


def main() -> int:
    """CLI entry point: apply the initial schema to ClickHouse."""
    log = create_logger("omniwatch.storage.clickhouse.migrations.001")
    try:
        statements = apply_schema(logger=log)
    except Exception as exc:  # noqa: BLE001 - CLI reports the root cause
        log.error("schema migration failed: %s", exc)
        return 1
    log.info("migration 001 applied %d statements", len(statements))
    return 0


if __name__ == "__main__":
    sys.exit(main())
