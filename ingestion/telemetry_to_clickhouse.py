"""
OmniWatch — Telemetry Ingestion Layer
Component: Telemetry-to-ClickHouse Bridge
Phase: 3
Purpose: Reads metrics from Prometheus AND logs from Loki, writes both to ClickHouse
Inputs: Prometheus API (metrics), Loki API (logs)
Outputs: ClickHouse omniwatch.metrics + omniwatch.logs tables
"""

import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "storage" / "clickhouse"))

from client import ClickHouseClient

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
LOKI_URL = os.getenv("LOKI_URL", "http://localhost:3100")


class TelemetryToClickHouseBridge:
    """
    Reads metrics from Prometheus and logs from Loki, writes both to ClickHouse.
    """

    def __init__(self):
        self._ch_client = ClickHouseClient()
        self._session = requests.Session()
        self._last_log_query = time.time()

    def start(self, interval: int = 30):
        """
        Start polling and writing to ClickHouse.

        Args:
            interval: Seconds between polls
        """
        if not self._ch_client.is_connected():
            print("[bridge] ERROR: ClickHouse not connected")
            return

        print(f"[bridge] Syncing Prometheus metrics + Loki logs every {interval}s")
        print("[bridge] Ctrl+C to stop")
        cycle = 0

        try:
            while True:
                cycle += 1
                self._sync_metrics()
                self._sync_logs()
                if cycle % 10 == 0:
                    print(f"[bridge] Cycle {cycle} — synced to ClickHouse")
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n[bridge] Stopped")

    def _sync_metrics(self):
        """Fetch metrics from Prometheus and write to ClickHouse."""
        try:
            query = '{__name__=~"omniwatch_.*"}'
            resp = self._session.get(
                f"{PROMETHEUS_URL}/api/v1/query",
                params={"query": query},
                timeout=10,
            )

            if resp.status_code != 200:
                return

            data = resp.json()
            if data.get("status") != "success":
                return

            metrics = []
            for result in data.get("data", {}).get("result", []):
                metric_name = result["metric"].get("__name__", "unknown")
                labels = {
                    k: v for k, v in result["metric"].items()
                    if k != "__name__"
                }
                entity_id = labels.get("service", "unknown")
                entity_type = labels.get("node_type", "UNKNOWN_NODE")
                value = float(result["value"][1])

                metrics.append({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "entity_id": entity_id,
                    "entity_type": entity_type,
                    "metric_name": metric_name,
                    "metric_value": value,
                    "labels": labels,
                    "source": "prometheus",
                })

            if metrics:
                self._ch_client.insert_metrics(metrics)

        except Exception as e:
            print(f"[bridge] Metrics sync error: {e}")

    def _sync_logs(self):
        """Fetch logs from Loki and write to ClickHouse."""
        try:
            # Query recent logs from Loki (last 60 seconds)
            end_ns = int(time.time() * 1e9)
            start_ns = end_ns - (60 * int(1e9))

            query = '{job="omniwatch-simulation"}'
            resp = self._session.get(
                f"{LOKI_URL}/loki/api/v1/query_range",
                params={
                    "query": query,
                    "start": str(start_ns),
                    "end": str(end_ns),
                    "limit": 100,
                },
                timeout=10,
            )

            if resp.status_code != 200:
                return

            data = resp.json()
            if data.get("status") != "success":
                return

            logs = []
            for stream in data.get("data", {}).get("result", []):
                stream_labels = stream.get("stream", {})
                entity_id = stream_labels.get("service", "unknown")
                entity_type = stream_labels.get("node_type", "UNKNOWN_NODE")
                log_level = stream_labels.get("level", "info")

                for ts_ns, message in stream.get("values", []):
                    # Convert nanosecond timestamp to datetime
                    ts_sec = int(ts_ns) / 1e9
                    ts_dt = datetime.fromtimestamp(ts_sec, tz=timezone.utc)

                    logs.append({
                        "timestamp": ts_dt.isoformat(),
                        "entity_id": entity_id,
                        "entity_type": entity_type,
                        "log_level": log_level,
                        "message": message,
                        "labels": {
                            "job": stream_labels.get("job", ""),
                            "cloud_provider": stream_labels.get("cloud_provider", ""),
                        },
                        "source": "loki",
                    })

            if logs:
                self._ch_client.insert_logs(logs)

        except Exception as e:
            print(f"[bridge] Logs sync error: {e}")


def main():
    bridge = TelemetryToClickHouseBridge()
    bridge.start()


if __name__ == "__main__":
    main()
