"""
OmniWatch — Unified Storage Layer
Component: ClickHouse Client
Phase: 4
Purpose: Connection helper for the feature-store API — lazily creates a shared
         clickhouse-connect client (its internal HTTP connection pool), applies
         3x exponential backoff (100ms -> 500ms -> 2s) on connect/query
         failures, and exposes a health-check method.
Inputs: CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_HTTP_PORT, CLICKHOUSE_DB,
        CLICKHOUSE_USER, CLICKHOUSE_PASSWORD env vars
Outputs: Feature-vector query results (list[dict]) and connectivity status
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import clickhouse_connect

# 3x exponential backoff between retries (initial attempt + 3 retries = 4 total
# attempts, matching the plan's "retry 3x 100ms -> 500ms -> 2s" contract).
RETRY_BACKOFF_SECONDS: List[float] = [0.1, 0.5, 2.0]

# Default time range when start/end are omitted: last 24 hours.
DEFAULT_WINDOW_HOURS = 24

# The 15-column feature_vectors schema (plan Task 14 / notepad learnings).
FEATURE_VECTOR_COLUMNS: List[str] = [
    "entity_id",
    "window_start",
    "window_end",
    "window_size",
    "latency_p50",
    "latency_p95",
    "latency_p99",
    "latency_avg",
    "latency_min",
    "latency_max",
    "error_rate",
    "request_volume",
    "feature_version",
    "ttl",
    "timestamp",
]


class ClickHouseUnavailable(Exception):
    """Raised when ClickHouse cannot be reached after all retry attempts."""


class JsonLogFormatter(logging.Formatter):
    """Structured JSON log formatter (AGENTS.md: stdout logs as JSON)."""

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class ClickHouseClient:
    """Client for the OmniWatch ClickHouse ``feature_vectors`` table.

    The underlying clickhouse-connect client manages its own HTTP connection
    pool, so the single shared instance IS the pool (thread-safe reuse).
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        connect_timeout: int = 10,
        send_receive_timeout: int = 30,
    ) -> None:
        self._host = host or os.getenv("CLICKHOUSE_HOST", "clickhouse")
        self._port = port if port is not None else self._resolve_port()
        self._database = database or os.getenv("CLICKHOUSE_DB", "omniwatch")
        self._username = username or os.getenv("CLICKHOUSE_USER", "default")
        self._password = (
            password if password is not None else os.getenv("CLICKHOUSE_PASSWORD", "")
        )
        self._connect_timeout = connect_timeout
        self._send_receive_timeout = send_receive_timeout
        self._client: Any = None
        self.logger = logging.getLogger("omniwatch.feature_store.clickhouse")

    @staticmethod
    def _resolve_port() -> int:
        """Port precedence: CLICKHOUSE_HTTP_PORT, then CLICKHOUSE_PORT.

        ``.env.example`` sets CLICKHOUSE_PORT=9000 (native) alongside
        CLICKHOUSE_HTTP_PORT=8123 (HTTP); clickhouse-connect speaks HTTP, so
        the HTTP port wins. Falls back to the task contract default 8123.
        """
        http_port = os.getenv("CLICKHOUSE_HTTP_PORT")
        if http_port:
            return int(http_port)
        return int(os.getenv("CLICKHOUSE_PORT", "8123"))

    # ------------------------------------------------------------------ #
    # Connection management
    # ------------------------------------------------------------------ #

    def get_client(self) -> Any:
        """Lazily create (once) and return the shared clickhouse-connect client."""
        if self._client is None:
            self._client = self._connect_with_retry()
        return self._client

    def _connect_with_retry(self) -> Any:
        last_error: Optional[Exception] = None
        total_attempts = len(RETRY_BACKOFF_SECONDS) + 1
        for attempt in range(total_attempts):
            try:
                self.logger.info(
                    "connecting to ClickHouse host=%s port=%s database=%s",
                    self._host,
                    self._port,
                    self._database,
                )
                client = clickhouse_connect.get_client(
                    host=self._host,
                    port=self._port,
                    database=self._database,
                    username=self._username,
                    password=self._password,
                    connect_timeout=self._connect_timeout,
                    send_receive_timeout=self._send_receive_timeout,
                )
                self.logger.info(
                    "connected to ClickHouse host=%s port=%s", self._host, self._port
                )
                return client
            except Exception as exc:  # noqa: BLE001 - any transport error retries
                last_error = exc
                if attempt < len(RETRY_BACKOFF_SECONDS):
                    wait = RETRY_BACKOFF_SECONDS[attempt]
                    self.logger.warning(
                        "clickhouse connect attempt %d/%d failed: %s; retrying in %.1fs",
                        attempt + 1,
                        total_attempts,
                        exc,
                        wait,
                    )
                    time.sleep(wait)
                else:
                    self.logger.error(
                        "clickhouse connect failed after %d attempts: %s",
                        total_attempts,
                        exc,
                    )
        raise ClickHouseUnavailable(
            f"could not connect to ClickHouse: {last_error}"
        ) from last_error

    def ping(self) -> bool:
        """Health check — True when ClickHouse answers a trivial query."""
        try:
            self.get_client().command("SELECT 1")
            return True
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("clickhouse health check failed: %s", exc)
            return False

    def close(self) -> None:
        """Release the underlying client (used in app shutdown)."""
        if self._client is not None:
            self._client.close()
            self._client = None

    # ------------------------------------------------------------------ #
    # feature_vectors queries
    # ------------------------------------------------------------------ #

    def query_features(
        self,
        entity_id: str,
        window_size: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """Query the ``feature_vectors`` table, returning rows as dicts.

        start/end default to [now - 24h, now] (naive UTC). Datetime values are
        serialized to ISO 8601 strings so the result is JSON-ready.
        """
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        start = start or (now - timedelta(hours=DEFAULT_WINDOW_HOURS))
        end = end or now
        if start > end:
            raise ValueError("start timestamp must be <= end timestamp")

        sql = (
            "SELECT entity_id, window_start, window_end, window_size, "
            "latency_p50, latency_p95, latency_p99, latency_avg, latency_min, "
            "latency_max, error_rate, request_volume, feature_version, ttl, timestamp "
            "FROM feature_vectors "
            "WHERE entity_id = %(entity_id)s "
            "AND window_start >= %(start)s AND window_end <= %(end)s"
        )
        params: Dict[str, Any] = {"entity_id": entity_id, "start": start, "end": end}
        if window_size:
            sql += " AND window_size = %(window_size)s"
            params["window_size"] = window_size
        sql += " ORDER BY window_start ASC"
        return self._query_with_retry(sql, params)

    def _query_with_retry(
        self, sql: str, params: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        last_error: Optional[Exception] = None
        total_attempts = len(RETRY_BACKOFF_SECONDS) + 1
        for attempt in range(total_attempts):
            try:
                result = self.get_client().query(sql, parameters=params)
                return self._rows_to_dicts(result)
            except Exception as exc:  # noqa: BLE001 - transient errors retry
                last_error = exc
                if attempt < len(RETRY_BACKOFF_SECONDS):
                    wait = RETRY_BACKOFF_SECONDS[attempt]
                    self.logger.warning(
                        "clickhouse query attempt %d/%d failed: %s; retrying in %.1fs",
                        attempt + 1,
                        total_attempts,
                        exc,
                        wait,
                    )
                    time.sleep(wait)
                else:
                    self.logger.error(
                        "clickhouse query failed after %d attempts: %s",
                        total_attempts,
                        exc,
                    )
        raise ClickHouseUnavailable(
            f"ClickHouse query failed: {last_error}"
        ) from last_error

    @staticmethod
    def _rows_to_dicts(result: Any) -> List[Dict[str, Any]]:
        """Row factory — return dicts keyed by query column name."""
        columns = list(result.column_names)
        rows = result.result_rows
        return [
            {col: ClickHouseClient._serialize(value) for col, value in zip(columns, row)}
            for row in rows
        ]

    @staticmethod
    def _serialize(value: Any) -> Any:
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        return value
