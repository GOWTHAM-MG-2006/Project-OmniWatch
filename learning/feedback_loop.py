"""
OmniWatch — Continuous Learning Layer
Component: Feedback Loop Processor
Phase: 11
Purpose: Consume ActionResult from omniwatch.remediation.actions Kafka topic,
         evaluate success, and write learning records to ClickHouse knowledge_base
         table with action_type and success_count columns.
Inputs: ActionResult JSON from Kafka (orchestration/models.py contract)
Outputs: knowledge_base rows with action_type and success_count populated
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

import clickhouse_connect
from confluent_kafka import Consumer, KafkaError, KafkaException

logger = logging.getLogger("omniwatch.learning.feedback_loop")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = os.environ.get("KAFKA_LEARNING_GROUP_ID", "omniwatch-learning-group")
KAFKA_AUTO_OFFSET_RESET = os.environ.get("KAFKA_AUTO_OFFSET_RESET", "earliest")

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "omniwatch")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")

REMEDIATION_TOPIC = "omniwatch.remediation.actions"

# Column list for knowledge_base insert (must match schema.sql + new columns).
KB_COLUMNS = [
    "kb_id",
    "incident_id",
    "root_cause_entity",
    "root_cause_entity_type",
    "resolution_summary",
    "actions_taken",
    "outcome",
    "action_type",
    "success_count",
    "created_at",
]

# SQL to ensure new columns exist at runtime (idempotent).
_ENSURE_COLUMNS_SQL = [
    "ALTER TABLE omniwatch.knowledge_base ADD COLUMN IF NOT EXISTS action_type String DEFAULT ''",
    "ALTER TABLE omniwatch.knowledge_base ADD COLUMN IF NOT EXISTS success_count UInt32 DEFAULT 0",
]


class FeedbackLoopProcessor:
    """Consume ActionResult records and persist learning entries to ClickHouse.

    Lifecycle:
        1. ``__init__`` — lazy Kafka consumer + ClickHouse client.
        2. ``start()`` — begin polling loop (blocks until stop signal).
        3. ``stop()`` — signal the loop to exit gracefully.
        4. ``process_message()`` — deserialise ActionResult, insert KB row.
    """

    def __init__(
        self,
        clickhouse_config: dict[str, Any] | None = None,
        kafka_config: dict[str, Any] | None = None,
    ) -> None:
        self._running = False

        # Kafka consumer (lazy init via start()).
        kafka_overrides = kafka_config or {}
        self._consumer_conf = {
            "bootstrap.servers": kafka_overrides.get("bootstrap_servers", KAFKA_BOOTSTRAP_SERVERS),
            "group.id": kafka_overrides.get("group_id", KAFKA_GROUP_ID),
            "auto.offset.reset": kafka_overrides.get("auto_offset_reset", KAFKA_AUTO_OFFSET_RESET),
            "enable.auto.commit": True,
        }
        self._consumer: Consumer | None = None

        # ClickHouse client (lazy init).
        ch_overrides = clickhouse_config or {}
        self._ch_host = ch_overrides.get("host", CLICKHOUSE_HOST)
        self._ch_port = int(ch_overrides.get("port", CLICKHOUSE_PORT))
        self._ch_db = ch_overrides.get("database", CLICKHOUSE_DB)
        self._ch_user = ch_overrides.get("username", CLICKHOUSE_USER)
        self._ch_password = ch_overrides.get("password", CLICKHOUSE_PASSWORD)
        self._ch_client: Any = None

    # ------------------------------------------------------------------ #
    # Connection helpers
    # ------------------------------------------------------------------ #

    def _get_kafka_consumer(self) -> Consumer:
        if self._consumer is None:
            self._consumer = Consumer(self._consumer_conf)
            self._consumer.subscribe([REMEDIATION_TOPIC])
            logger.info("subscribed to %s", REMEDIATION_TOPIC)
        return self._consumer

    def _get_ch_client(self) -> Any:
        if self._ch_client is None:
            self._ch_client = clickhouse_connect.get_client(
                host=self._ch_host,
                port=self._ch_port,
                database=self._ch_db,
                username=self._ch_user,
                password=self._ch_password,
            )
            self._ensure_columns()
        return self._ch_client

    def _ensure_columns(self) -> None:
        """Add action_type and success_count columns if they don't exist yet."""
        for sql in _ENSURE_COLUMNS_SQL:
            try:
                self._ch_client.command(sql)
            except Exception as exc:  # noqa: BLE001
                logger.warning("column ensure failed: %s — %s", sql, exc)

    # ------------------------------------------------------------------ #
    # Core processing
    # ------------------------------------------------------------------ #

    def process_message(self, raw_value: bytes) -> bool:
        """Deserialize an ActionResult and insert a knowledge_base row.

        Returns True on success, False on any failure.
        """
        try:
            action = json.loads(raw_value)
        except (json.JSONDecodeError, TypeError) as exc:
            logger.error("failed to deserialize ActionResult: %s", exc)
            return False

        kb_row = self._build_kb_row(action)
        try:
            client = self._get_ch_client()
            data = [self._normalize_kb_row(kb_row)]
            client.insert("omniwatch.knowledge_base", data, column_names=KB_COLUMNS)
            logger.info(
                "inserted knowledge_base kb_id=%s incident_id=%s action_type=%s success=%s",
                kb_row["kb_id"],
                kb_row["incident_id"],
                kb_row["action_type"],
                action.get("success"),
            )
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error("failed to insert knowledge_base: %s", exc)
            return False

    def _build_kb_row(self, action: dict[str, Any]) -> dict[str, Any]:
        """Map an ActionResult dict to a knowledge_base row."""
        success_count = 1 if action.get("success") else 0
        return {
            "kb_id": str(uuid.uuid4()),
            "incident_id": action.get("incident_id", ""),
            "root_cause_entity": action.get("entity_id", ""),
            "root_cause_entity_type": action.get("entity_type", ""),
            "resolution_summary": action.get("output", ""),
            "actions_taken": json.dumps([action.get("action_type", "")]),
            "outcome": "success" if action.get("success") else "failure",
            "action_type": action.get("action_type", ""),
            "success_count": success_count,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

    @staticmethod
    def _normalize_kb_row(row: dict[str, Any]) -> list[Any]:
        """Column-align a KB row for clickhouse-connect insert."""
        out: list[Any] = []
        for col in KB_COLUMNS:
            value = row.get(col)
            if col == "created_at" and isinstance(value, str):
                try:
                    value = datetime.fromisoformat(value)
                except (TypeError, ValueError):
                    pass
            out.append(value)
        return out

    # ------------------------------------------------------------------ #
    # Polling loop
    # ------------------------------------------------------------------ #

    def start(self, poll_interval: float = 1.0) -> None:
        """Block and poll Kafka until ``stop()`` is called.

        Parameters
        ----------
        poll_interval:
            Seconds to wait per ``consumer.poll()`` call.
        """
        self._running = True
        consumer = self._get_kafka_consumer()
        logger.info("feedback loop started, polling every %.1fs", poll_interval)

        try:
            while self._running:
                msg = consumer.poll(poll_interval)
                if msg is None:
                    continue
                err = msg.error()
                if err is not None:
                    if err.code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("kafka error: %s", err)
                    continue
                value = msg.value()
                if value is None:
                    continue
                self.process_message(value)
        except KafkaException as exc:
            logger.error("kafka exception in polling loop: %s", exc)
        finally:
            self.stop()
            logger.info("feedback loop stopped")

    def stop(self) -> None:
        """Signal the polling loop to exit and close connections."""
        self._running = False
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception:  # noqa: BLE001, S110
                pass
            self._consumer = None
        if self._ch_client is not None:
            try:
                self._ch_client.close()
            except Exception:  # noqa: BLE001, S110
                pass
            self._ch_client = None


def main() -> None:
    """Entry point for ``python -m learning.feedback_loop``."""
    processor = FeedbackLoopProcessor()

    def _handle_signal(signum: int, _frame: Any) -> None:
        logger.info("received signal %s, stopping", signum)
        processor.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    processor.start()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    main()
