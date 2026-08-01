"""
OmniWatch — Unified Storage Layer
Component: Configuration
Phase: 5
Purpose: Centralized connection configuration for the three storage backends
         (ClickHouse, Neo4j, MinIO), loaded from environment variables with
         defaults that match docker-compose.yml for local development.
Inputs: CLICKHOUSE_HOST/PORT/DB/USER/PASSWORD, NEO4J_URI/USER/PASSWORD,
        MINIO_ENDPOINT/ACCESS_KEY/SECRET_KEY/SECURE/CONSOLE_PORT env vars
Outputs: A StorageConfig dataclass instance (via StorageConfig.from_env())
         consumed by the storage/clickhouse, storage/neo4j, and storage/minio
         clients in later Phase 5 tasks
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class StorageConfig:
    """Connection parameters for all Unified Storage Layer backends.

    Every field defaults to the value from docker-compose.yml (local dev),
    and is overridable via its matching environment variable. Never store
    secrets in code — passwords are only ever env-var defaults for the
    local dev compose stack.
    """

    # ------------------------------------------------------------------ #
    # ClickHouse (omniwatch-clickhouse) — HTTP port 8123
    # ------------------------------------------------------------------ #
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_db: str = "omniwatch"
    clickhouse_user: str = "default"
    clickhouse_password: str = ""

    # ------------------------------------------------------------------ #
    # Neo4j (omniwatch-neo4j) — bolt 7687 / browser 7474
    # ------------------------------------------------------------------ #
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "omniwatch"

    # ------------------------------------------------------------------ #
    # MinIO (omniwatch-minio) — API :9010, console :9001
    # ------------------------------------------------------------------ #
    minio_endpoint: str = "localhost:9010"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_secure: bool = False
    minio_console_port: int = 9001

    # ------------------------------------------------------------------ #
    # Env-var names per backend (single source of truth for from_env)
    # ------------------------------------------------------------------ #
    _env_map: Dict[str, str] = field(default_factory=dict, init=False, repr=False)

    @classmethod
    def from_env(cls) -> "StorageConfig":
        """Build a StorageConfig from environment variables.

        Unset variables fall back to the docker-compose defaults. Numeric
        (port) and boolean (MINIO_SECURE) fields are parsed to their typed
        value; malformed values raise ValueError rather than silently
        misbehaving at connect time.
        """
        cfg = cls()
        cfg.clickhouse_host = os.getenv("CLICKHOUSE_HOST", cfg.clickhouse_host)
        cfg.clickhouse_port = int(os.getenv("CLICKHOUSE_PORT", str(cfg.clickhouse_port)))
        cfg.clickhouse_db = os.getenv("CLICKHOUSE_DB", cfg.clickhouse_db)
        cfg.clickhouse_user = os.getenv("CLICKHOUSE_USER", cfg.clickhouse_user)
        cfg.clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", cfg.clickhouse_password)

        cfg.neo4j_uri = os.getenv("NEO4J_URI", cfg.neo4j_uri)
        cfg.neo4j_user = os.getenv("NEO4J_USER", cfg.neo4j_user)
        cfg.neo4j_password = os.getenv("NEO4J_PASSWORD", cfg.neo4j_password)

        cfg.minio_endpoint = os.getenv("MINIO_ENDPOINT", cfg.minio_endpoint)
        cfg.minio_access_key = os.getenv("MINIO_ACCESS_KEY", cfg.minio_access_key)
        cfg.minio_secret_key = os.getenv("MINIO_SECRET_KEY", cfg.minio_secret_key)
        cfg.minio_secure = os.getenv("MINIO_SECURE", str(cfg.minio_secure)).lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
        cfg.minio_console_port = int(
            os.getenv("MINIO_CONSOLE_PORT", str(cfg.minio_console_port))
        )

        cfg._env_map = {
            "CLICKHOUSE_HOST": cfg.clickhouse_host,
            "CLICKHOUSE_PORT": str(cfg.clickhouse_port),
            "CLICKHOUSE_DB": cfg.clickhouse_db,
            "CLICKHOUSE_USER": cfg.clickhouse_user,
            "CLICKHOUSE_PASSWORD": cfg.clickhouse_password,
            "NEO4J_URI": cfg.neo4j_uri,
            "NEO4J_USER": cfg.neo4j_user,
            "NEO4J_PASSWORD": cfg.neo4j_password,
            "MINIO_ENDPOINT": cfg.minio_endpoint,
            "MINIO_ACCESS_KEY": cfg.minio_access_key,
            "MINIO_SECRET_KEY": cfg.minio_secret_key,
            "MINIO_SECURE": str(cfg.minio_secure),
            "MINIO_CONSOLE_PORT": str(cfg.minio_console_port),
        }
        return cfg

    def env(self) -> Dict[str, str]:
        """Return the effective env-var mapping for this config instance.

        Useful for health checks / diagnostics and for the E2E test that
        asserts the storage layer boots against the same values.
        """
        return self._env_map
