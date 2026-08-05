"""
OmniWatch — Unified Storage Layer
Component: ClickHouse Client
Phase: 5
Purpose: Batched insert + query client for the ClickHouse Unified Storage Layer
         (omniwatch.metrics / logs / anomalies / incidents). Lazily builds a
         shared clickhouse-connect client (its internal HTTP connection pool is
         the pool), retries connect/query failures 3x with exponential backoff
         (100ms -> 500ms -> 2s) via storage.common.retry_with_backoff, and
         exposes per-table row-count stats and a health check.
Inputs: Telemetry/anomaly/incident row dicts (column names must match
        storage/clickhouse/schema.sql); StorageConfig via StorageConfig.from_env()
Outputs: Inserted row counts (int), entity-scoped rows (list[dict]),
         per-table row-count map (dict[str, int]), connectivity status (bool)
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Optional

import clickhouse_connect

from ..common import StorageError, create_logger, retry_with_backoff
from ..config import StorageConfig

# Column lists MUST match storage/clickhouse/schema.sql exactly (task contract).
METRICS_COLUMNS: list[str] = [
    "entity_id",
    "entity_type",
    "metric_name",
    "value",
    "tags",
    "source_type",
    "timestamp",
]
LOGS_COLUMNS: list[str] = [
    "entity_id",
    "log_level",
    "message",
    "service_name",
    "trace_id",
    "timestamp",
]
ANOMALIES_COLUMNS: list[str] = [
    "anomaly_id",
    "entity_id",
    "entity_type",
    "metric_name",
    "anomaly_score",
    "confidence",
    "deviation_from_baseline",
    "source_type",
    "status",
    "timestamp",
    "attack_type",
    "severity",
    "evidence_logs",
    "recommended_action",
    "source_ip",
]
INCIDENTS_COLUMNS: list[str] = [
    "incident_id",
    "severity",
    "business_impact_score",
    "root_cause_entity",
    "entity_type",
    "confidence",
    "fault_path",
    "impacted_services",
    "status",
    "deduplicated_count",
    "sla_breach_risk",
    "assigned_to",
    "created_at",
]
PENDING_APPROVALS_COLUMNS: list[str] = [
    "approval_id",
    "incident_id",
    "action_type",
    "entity_id",
    "proposed_by",
    "status",
    "created_at",
    "decided_at",
]

# List-valued columns stored as JSON strings (schema.sql stores serialized arrays).
INCIDENTS_JSON_COLUMNS: set[str] = {"fault_path", "impacted_services"}
ANOMALIES_JSON_COLUMNS: set[str] = {"evidence_logs"}

# Map(String, String) columns — accept a dict, or a pre-serialized JSON object.
MAP_COLUMNS: set[str] = {"tags"}

# DateTime columns — parse ISO-8601 string values to datetime (client requires datetime).
TIMESTAMP_COLUMNS: set[str] = {"timestamp", "created_at", "decided_at"}

# Tables reachable via select_by_entity (whitelist guards SQL interpolation).
SELECT_TABLES: set[str] = {
    "metrics",
    "logs",
    "traces",
    "anomalies",
    "incidents",
    "pending_approvals",
    "knowledge_base",
    "feature_vectors",
}

# Tables whose time column is created_at (no timestamp column in schema.sql).
CREATED_AT_TABLES: set[str] = {"incidents", "pending_approvals", "knowledge_base"}


class ClickHouseClient:
    """Client for the OmniWatch ClickHouse storage tables.

    The underlying clickhouse-connect client manages its own HTTP connection
    pool, so the single shared instance IS the pool (thread-safe reuse), and
    every connect/query/insert is wrapped in the storage layer's 3x
    exponential-backoff retry (100ms -> 500ms -> 2s).
    """

    def __init__(
        self,
        config: Optional[StorageConfig] = None,
        *,
        connect_timeout: int = 10,
        send_receive_timeout: int = 30,
    ) -> None:
        cfg = config or StorageConfig.from_env()
        self._host = cfg.clickhouse_host
        self._port = cfg.clickhouse_port
        self._database = cfg.clickhouse_db
        self._username = cfg.clickhouse_user
        self._password = cfg.clickhouse_password
        self._connect_timeout = connect_timeout
        self._send_receive_timeout = send_receive_timeout
        self._client: Any = None
        self.logger = create_logger("omniwatch.storage.clickhouse")

    # ------------------------------------------------------------------ #
    # Connection management
    # ------------------------------------------------------------------ #

    def get_client(self) -> Any:
        """Lazily create (once) and return the shared clickhouse-connect client."""
        if self._client is None:
            self._client = self._connect_with_retry()
        return self._client

    def _connect_with_retry(self) -> Any:
        def _connect() -> Any:
            self.logger.info(
                "connecting to ClickHouse host=%s port=%s database=%s",
                self._host,
                self._port,
                self._database,
            )
            return clickhouse_connect.get_client(
                host=self._host,
                port=self._port,
                database=self._database,
                username=self._username,
                password=self._password,
                connect_timeout=self._connect_timeout,
                send_receive_timeout=self._send_receive_timeout,
            )

        try:
            client = retry_with_backoff(_connect, logger=self.logger)
        except Exception as exc:  # noqa: BLE001 - surface as StorageError
            raise StorageError(
                f"could not connect to ClickHouse at {self._host}:{self._port}: {exc}"
            ) from exc
        self.logger.info("connected to ClickHouse host=%s port=%s", self._host, self._port)
        return client

    def _with_retry(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        """Invoke ``func`` with the storage layer's 3x exponential backoff."""
        return retry_with_backoff(func, *args, logger=self.logger, **kwargs)

    def close(self) -> None:
        """Release the underlying client (used in app shutdown)."""
        if self._client is not None:
            self._client.close()
            self._client = None

    # ------------------------------------------------------------------ #
    # Health / stats
    # ------------------------------------------------------------------ #

    def health_check(self) -> bool:
        """True when ClickHouse answers a trivial ``SELECT 1``."""
        try:
            self._with_retry(lambda: self._command("SELECT 1"))
            return True
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("clickhouse health check failed: %s", exc)
            return False

    def get_table_stats(self) -> dict[str, int]:
        """Row count per table, discovered via ``SHOW TABLES`` + ``SELECT count(*)``."""
        def _stats() -> dict[str, int]:
            client = self.get_client()
            tables = client.query("SHOW TABLES").result_rows
            stats: dict[str, int] = {}
            for row in tables:
                if not row:
                    continue
                name = str(row[0])
                count = client.query(f"SELECT count(*) FROM omniwatch.{name}").result_rows[0][0]
                stats[name] = int(count)
            return stats

        return self._with_retry(_stats)

    def _command(self, sql: str) -> Any:
        return self.get_client().command(sql)

    # ------------------------------------------------------------------ #
    # Batched inserts
    # ------------------------------------------------------------------ #

    def insert_metrics(self, rows: list[dict[str, Any]]) -> int:
        """Batched insert into ``omniwatch.metrics``; returns inserted row count."""
        return self._insert("metrics", rows, METRICS_COLUMNS, json_columns=set())

    def insert_logs(self, rows: list[dict[str, Any]]) -> int:
        """Batched insert into ``omniwatch.logs``; returns inserted row count."""
        return self._insert("logs", rows, LOGS_COLUMNS, json_columns=set())

    def insert_anomalies(self, rows: list[dict[str, Any]]) -> int:
        """Batched insert into ``omniwatch.anomalies``; returns inserted row count.

        ``evidence_logs`` list values are serialized to JSON strings to match
        the schema's String column.
        """
        return self._insert(
            "anomalies", rows, ANOMALIES_COLUMNS, json_columns=ANOMALIES_JSON_COLUMNS
        )

    def insert_incidents(self, rows: list[dict[str, Any]]) -> int:
        """Batched insert into ``omniwatch.incidents``; returns inserted row count.

        ``fault_path`` / ``impacted_services`` list values are serialized to JSON
        strings to match the schema's String columns.
        """
        return self._insert(
            "incidents", rows, INCIDENTS_COLUMNS, json_columns=INCIDENTS_JSON_COLUMNS
        )

    def insert_pending_approvals(self, rows: list[dict[str, Any]]) -> int:
        """Batched insert into ``omniwatch.pending_approvals``; returns inserted row count."""
        return self._insert(
            "pending_approvals", rows, PENDING_APPROVALS_COLUMNS, json_columns=set()
        )

    def _insert(
        self,
        table: str,
        rows: list[dict[str, Any]],
        columns: list[str],
        json_columns: set[str],
    ) -> int:
        if not rows:
            self.logger.debug("skipping empty insert into omniwatch.%s", table)
            return 0
        data = [
            ClickHouseClient._normalize_row(row, columns, json_columns) for row in rows
        ]

        def _do_insert() -> None:
            client = self.get_client()
            client.insert(f"omniwatch.{table}", data, column_names=columns)

        self._with_retry(_do_insert)
        self.logger.info("inserted %d rows into omniwatch.%s", len(rows), table)
        return len(rows)

    @staticmethod
    def _normalize_row(
        row: dict[str, Any], columns: list[str], json_columns: set[str]
    ) -> list[Any]:
        """Column-align a row, serializing list/dict values where required.

        ``json_columns`` values that are dicts/lists are JSON-encoded (schema
        String columns that store serialized arrays); Map columns accept either
        a dict or a pre-serialized JSON object.
        """
        out: list[Any] = []
        for col in columns:
            value = row.get(col)
            if value is not None and col in json_columns and isinstance(value, (dict, list)):
                out.append(json.dumps(value))
            elif col in MAP_COLUMNS and isinstance(value, str):
                try:
                    parsed = json.loads(value)
                    out.append(parsed if isinstance(parsed, dict) else value)
                except (TypeError, ValueError):
                    out.append(value)
            elif col in TIMESTAMP_COLUMNS and isinstance(value, str):
                try:
                    out.append(datetime.fromisoformat(value))
                except (TypeError, ValueError):
                    out.append(value)
            else:
                out.append(value)
        return out

    # ------------------------------------------------------------------ #
    # Queries
    # ------------------------------------------------------------------ #

    def select_by_entity(
        self,
        entity_id: str,
        table: str = "metrics",
        limit: int = 100,
        order_by: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Return up to ``limit`` rows for an entity, newest first.

        Default order column is ``timestamp`` except for the created_at-only
        tables (incidents, pending_approvals, knowledge_base), which use
        ``created_at``. ``table`` and ``order_by`` are whitelist-validated to
        keep the interpolated identifiers injection-safe.
        """
        if table not in SELECT_TABLES:
            raise ValueError(
                f"table must be one of {sorted(SELECT_TABLES)}, got {table!r}"
            )
        if order_by is not None and not order_by.replace("_", "").isalnum():
            raise ValueError(f"invalid order_by column: {order_by!r}")
        order_col = order_by or (
            "created_at" if table in CREATED_AT_TABLES else "timestamp"
        )
        sql = (
            f"SELECT * FROM omniwatch.{table} "
            "WHERE entity_id = %(entity_id)s "
            f"ORDER BY {order_col} DESC LIMIT %(limit)s"
        )
        params: dict[str, Any] = {"entity_id": entity_id, "limit": limit}
        return self._with_retry(lambda: self._query(sql, params))

    def select_metrics_baseline(
        self,
        entity_id: str,
        start: datetime,
        end: datetime,
        metric_name: str,
    ) -> dict[str, Any]:
        """Return aggregated baseline stats for a metric over a time range.

        Queries ``omniwatch.metrics`` and computes avg, stddev (sample),
        95th-percentile, and row count for the given entity, metric, and
        time window.

        Returns ``{"avg": float, "stddev": float, "p95": float, "count": int}``.
        When no rows match, all values default to 0.
        """
        if not metric_name or not metric_name.replace("_", "").replace(".", "").isalnum():
            raise ValueError(f"invalid metric_name: {metric_name!r}")
        sql = (
            "SELECT "
            "  avg(value) AS avg, "
            "  stddevSamp(value) AS stddev, "
            "  quantile(0.95)(value) AS p95, "
            "  count(*) AS cnt "
            "FROM omniwatch.metrics "
            "WHERE entity_id = %(entity_id)s "
            "  AND metric_name = %(metric_name)s "
            "  AND timestamp >= %(start)s "
            "  AND timestamp <= %(end)s"
        )
        params: dict[str, Any] = {
            "entity_id": entity_id,
            "metric_name": metric_name,
            "start": start,
            "end": end,
        }
        rows = self._with_retry(lambda: self._query(sql, params))
        if not rows:
            return {"avg": 0.0, "stddev": 0.0, "p95": 0.0, "count": 0}
        row = rows[0]
        return {
            "avg": float(row.get("avg") or 0.0),
            "stddev": float(row.get("stddev") or 0.0),
            "p95": float(row.get("p95") or 0.0),
            "count": int(row.get("cnt") or 0),
        }

    def _query(self, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        client = self.get_client()
        result = client.query(sql, parameters=params)
        columns = list(result.column_names)
        return [
            {
                col: ClickHouseClient._serialize(value)
                for col, value in zip(columns, row)
            }
            for row in result.result_rows
        ]

    @staticmethod
    def _serialize(value: Any) -> Any:
        """Serialize datetime values to ISO 8601 so results are JSON-ready."""
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        return value


# ------------------------------------------------------------------ #
# Module-level convenience functions
# ------------------------------------------------------------------ #


def insert_pending_approvals(record: dict[str, Any]) -> int:
    """Insert a single pending-approval record into ``omniwatch.pending_approvals``.

    Module-level convenience — creates a temporary :class:`ClickHouseClient`,
    delegates to the batched ``_insert`` path, and closes the client.  Returns
    the inserted row count (always 1 on success).
    """
    logger = create_logger("omniwatch.storage.clickhouse")
    client = ClickHouseClient()
    try:
        result = client._insert(
            "pending_approvals",
            [record],
            PENDING_APPROVALS_COLUMNS,
            json_columns=set(),
        )
        logger.info(
            "inserted pending approval approval_id=%s",
            record.get("approval_id"),
        )
        return result
    except Exception as exc:
        logger.error(
            "failed to insert pending approval approval_id=%s error=%s",
            record.get("approval_id"),
            exc,
        )
        raise
    finally:
        client.close()
