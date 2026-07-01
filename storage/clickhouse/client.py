"""
OmniWatch — Unified Storage Layer
Component: ClickHouse Client
Phase: 3
Purpose: ClickHouse read/write client for metrics, logs, anomalies, incidents
Inputs: Structured events from abstraction layer
Outputs: ClickHouse tables (metrics, logs, anomalies, incidents)
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------------
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "omniwatch")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")


class ClickHouseClient:
    """
    ClickHouse client for OmniWatch storage operations.

    Usage:
        client = ClickHouseClient()
        client.insert_metrics([metric_dict1, metric_dict2])
        results = client.query("SELECT * FROM metrics LIMIT 10")
    """

    def __init__(self):
        """Initialize ClickHouse connection."""
        self._client = None
        self._connected = False
        self._connect()

    def _connect(self):
        """Establish connection to ClickHouse."""
        try:
            from clickhouse_driver import Client
            self._client = Client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                database=CLICKHOUSE_DB,
                user=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
            )
            # Test connection
            self._client.execute("SELECT 1")
            self._connected = True
            print(f"[clickhouse] Connected to {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}")
        except ImportError:
            print("[clickhouse] WARNING: clickhouse_driver not installed. Install with: pip install clickhouse-driver")
            self._connected = False
        except Exception as e:
            print(f"[clickhouse] WARNING: Connection failed: {e}")
            self._connected = False

    def is_connected(self) -> bool:
        """Check if connected to ClickHouse."""
        return self._connected

    def execute_schema(self, schema_path: str = None):
        """Execute schema.sql to create all tables."""
        if not self._connected:
            print("[clickhouse] Not connected — cannot execute schema")
            return False

        if schema_path is None:
            schema_path = Path(__file__).parent / "schema.sql"

        try:
            with open(schema_path) as f:
                schema_sql = f.read()

            # Split by semicolons and execute each statement
            statements = [s.strip() for s in schema_sql.split(";") if s.strip()]
            for stmt in statements:
                if stmt:
                    self._client.execute(stmt)

            print("[clickhouse] Schema executed successfully")
            return True
        except Exception as e:
            print(f"[clickhouse] Schema execution failed: {e}")
            return False

    def insert_metrics(self, metrics: list):
        """
        Insert metric records into the metrics table.

        Args:
            metrics: List of dicts with keys: timestamp, entity_id, entity_type,
                     metric_name, metric_value, labels, source
        """
        if not self._connected or not metrics:
            return False

        try:
            rows = []
            for m in metrics:
                rows.append((
                    m.get("timestamp", datetime.now(timezone.utc).isoformat()),
                    m.get("entity_id", "unknown"),
                    m.get("entity_type", "UNKNOWN_NODE"),
                    m.get("metric_name", "unknown"),
                    float(m.get("metric_value", 0.0)),
                    m.get("labels", {}),
                    m.get("source", "simulation"),
                ))

            self._client.execute(
                "INSERT INTO metrics (timestamp, entity_id, entity_type, metric_name, metric_value, labels, source) VALUES",
                rows,
            )
            return True
        except Exception as e:
            print(f"[clickhouse] Insert metrics failed: {e}")
            return False

    def insert_logs(self, logs: list):
        """
        Insert log records into the logs table.

        Args:
            logs: List of dicts with keys: timestamp, entity_id, entity_type,
                  log_level, message, labels, source
        """
        if not self._connected or not logs:
            return False

        try:
            rows = []
            for l in logs:
                rows.append((
                    l.get("timestamp", datetime.now(timezone.utc).isoformat()),
                    l.get("entity_id", "unknown"),
                    l.get("entity_type", "UNKNOWN_NODE"),
                    l.get("log_level", "info"),
                    l.get("message", ""),
                    l.get("labels", {}),
                    l.get("source", "simulation"),
                ))

            self._client.execute(
                "INSERT INTO logs (timestamp, entity_id, entity_type, log_level, message, labels, source) VALUES",
                rows,
            )
            return True
        except Exception as e:
            print(f"[clickhouse] Insert logs failed: {e}")
            return False

    def insert_anomalies(self, anomalies: list):
        """
        Insert anomaly records into the anomalies table.

        Args:
            anomalies: List of dicts with keys: timestamp, entity_id,
                       anomaly_score, confidence, metric_name, anomaly_type, status
        """
        if not self._connected or not anomalies:
            return False

        try:
            rows = []
            for a in anomalies:
                rows.append((
                    a.get("timestamp", datetime.now(timezone.utc).isoformat()),
                    a.get("entity_id", "unknown"),
                    float(a.get("anomaly_score", 0.0)),
                    float(a.get("confidence", 0.0)),
                    a.get("metric_name", "unknown"),
                    a.get("anomaly_type", "unknown"),
                    a.get("status", "active"),
                ))

            self._client.execute(
                "INSERT INTO anomalies (timestamp, entity_id, anomaly_score, confidence, metric_name, anomaly_type, status) VALUES",
                rows,
            )
            return True
        except Exception as e:
            print(f"[clickhouse] Insert anomalies failed: {e}")
            return False

    def insert_incidents(self, incidents: list):
        """
        Insert incident records into the incidents table.

        Args:
            incidents: List of dicts with keys: created_at, severity,
                       business_impact_score, root_cause_entity, status,
                       resolution_time, auto_resolved
        """
        if not self._connected or not incidents:
            return False

        try:
            rows = []
            for i in incidents:
                rows.append((
                    i.get("created_at", datetime.now(timezone.utc).isoformat()),
                    i.get("severity", "P4"),
                    float(i.get("business_impact_score", 0.0)),
                    i.get("root_cause_entity", "unknown"),
                    i.get("status", "OPEN"),
                    i.get("resolution_time"),
                    bool(i.get("auto_resolved", False)),
                ))

            self._client.execute(
                "INSERT INTO incidents (created_at, severity, business_impact_score, root_cause_entity, status, resolution_time, auto_resolved) VALUES",
                rows,
            )
            return True
        except Exception as e:
            print(f"[clickhouse] Insert incidents failed: {e}")
            return False

    def query(self, sql: str, params: dict = None) -> list:
        """
        Execute a SQL query and return results as list of dicts.

        Args:
            sql: SQL query string
            params: Optional query parameters

        Returns:
            List of dicts representing query results
        """
        if not self._connected:
            return []

        try:
            result = self._client.execute(sql, params or {}, with_column_types=True)
            rows, columns = result
            if not columns:
                return []
            col_names = [c[0] for c in columns]
            return [dict(zip(col_names, row)) for row in rows]
        except Exception as e:
            print(f"[clickhouse] Query failed: {e}")
            return []

    def count_records(self, table: str) -> int:
        """Count records in a table."""
        result = self.query(f"SELECT count() as cnt FROM {table}")
        return result[0]["cnt"] if result else 0

    def close(self):
        """Close the ClickHouse connection."""
        if self._client:
            self._client.disconnect()
            self._connected = False


# ---------------------------------------------------------------------------
# CLI interface
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="OmniWatch ClickHouse Client")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("schema", help="Execute schema.sql")
    subparsers.add_parser("status", help="Check connection and table counts")

    query_parser = subparsers.add_parser("query", help="Run a SQL query")
    query_parser.add_argument("--sql", required=True, help="SQL query")

    args = parser.parse_args()
    client = ClickHouseClient()

    if args.command == "schema":
        client.execute_schema()

    elif args.command == "status":
        print(f"Connected: {client.is_connected()}")
        if client.is_connected():
            for table in ["metrics", "logs", "anomalies", "incidents"]:
                count = client.count_records(table)
                print(f"  {table}: {count} records")

    elif args.command == "query":
        results = client.query(args.sql)
        print(json.dumps(results, indent=2, default=str))

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
