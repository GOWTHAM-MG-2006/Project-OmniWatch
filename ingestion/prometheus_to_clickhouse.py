"""
OmniWatch — Telemetry Ingestion Layer
Component: Prometheus-to-ClickHouse Bridge
Phase: 3
Purpose: Reads metrics from Prometheus and writes to ClickHouse
Inputs: Prometheus API (http://localhost:9090)
Outputs: ClickHouse omniwatch.metrics table
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "storage"))

from clickhouse.client import ClickHouseClient

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")


class PrometheusToClickHouseBridge:
    """
    Reads metrics from Prometheus and writes to ClickHouse.
    """

    def __init__(self):
        self._ch_client = ClickHouseClient()
        self._session = requests.Session()

    def start(self, interval: int = 30):
        """
        Start polling Prometheus and writing to ClickHouse.

        Args:
            interval: Seconds between polls
        """
        if not self._ch_client.is_connected():
            print("[prom-bridge] ERROR: ClickHouse not connected")
            return

        print(f"[prom-bridge] Polling Prometheus every {interval}s — Ctrl+C to stop")
        cycle = 0

        try:
            while True:
                cycle += 1
                self._sync_metrics()
                if cycle % 10 == 0:
                    print(f"[prom-bridge] Cycle {cycle} — synced metrics to ClickHouse")
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n[prom-bridge] Stopped")

    def _sync_metrics(self):
        """Fetch metrics from Prometheus and write to ClickHouse."""
        try:
            # Query all omniwatch metrics
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
            print(f"[prom-bridge] Sync error: {e}")


def main():
    bridge = PrometheusToClickHouseBridge()
    bridge.start()


if __name__ == "__main__":
    main()
