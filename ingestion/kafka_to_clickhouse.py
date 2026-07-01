"""
OmniWatch — Telemetry Ingestion Layer
Component: Kafka-to-ClickHouse Bridge
Phase: 3
Purpose: Consumes metrics from Kafka and writes to ClickHouse storage
Inputs: Kafka topic omniwatch.metrics.raw
Outputs: ClickHouse omniwatch.metrics table
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

# Add paths for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingestion"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from kafka_bus import KafkaConsumer
from storage.clickhouse.client import ClickHouseClient

load_dotenv(Path(__file__).resolve().parent.parent / ".env")


class KafkaToClickHouseBridge:
    """
    Consumes messages from Kafka and writes to ClickHouse.
    """

    def __init__(self):
        self._consumer = KafkaConsumer(group_id="clickhouse-writer")
        self._ch_client = ClickHouseClient()

    def start(self, topics: list = None):
        """
        Start consuming from Kafka and writing to ClickHouse.

        Args:
            topics: List of Kafka topics to consume from
        """
        if topics is None:
            topics = ["omniwatch.metrics.raw", "omniwatch.logs.raw"]

        if not self._ch_client.is_connected():
            print("[bridge] ERROR: ClickHouse not connected")
            return

        self._consumer.subscribe(topics)
        print(f"[bridge] Consuming from: {', '.join(topics)}")
        print("[bridge] Writing to ClickHouse — Ctrl+C to stop")

        try:
            while True:
                messages = self._consumer.consume(timeout=2.0)
                for msg in messages:
                    self._process_message(msg)
        except KeyboardInterrupt:
            print("\n[bridge] Stopped")
        finally:
            self._consumer.close()

    def _process_message(self, msg: dict):
        """Process a single Kafka message and write to ClickHouse."""
        topic = msg.get("topic", "")
        value = msg.get("value", {})

        if not isinstance(value, dict):
            return

        try:
            if topic == "omniwatch.metrics.raw":
                self._write_metric(value)
            elif topic == "omniwatch.logs.raw":
                self._write_log(value)
        except Exception as e:
            print(f"[bridge] Write failed: {e}")

    def _write_metric(self, data: dict):
        """Write a metric to ClickHouse."""
        metric = {
            "timestamp": data.get("timestamp", datetime.now(timezone.utc).isoformat()),
            "entity_id": data.get("entity_id", "unknown"),
            "entity_type": data.get("entity_type", "UNKNOWN_NODE"),
            "metric_name": data.get("metric_name", "unknown"),
            "metric_value": float(data.get("metric_value", 0.0)),
            "labels": data.get("labels", {}),
            "source": data.get("source", "kafka"),
        }
        self._ch_client.insert_metrics([metric])

    def _write_log(self, data: dict):
        """Write a log to ClickHouse."""
        log = {
            "timestamp": data.get("timestamp", datetime.now(timezone.utc).isoformat()),
            "entity_id": data.get("entity_id", "unknown"),
            "entity_type": data.get("entity_type", "UNKNOWN_NODE"),
            "log_level": data.get("log_level", "info"),
            "message": data.get("message", ""),
            "labels": data.get("labels", {}),
            "source": data.get("source", "kafka"),
        }
        self._ch_client.insert_logs([log])


def main():
    bridge = KafkaToClickHouseBridge()
    bridge.start()


if __name__ == "__main__":
    main()
