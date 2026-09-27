"""
OmniWatch — Ingestion Layer
Component: Workspace Bridge Consumer
Phase: omni-agent-page
Purpose: Consume shared OTLP raw Kafka topics, keep only signals tagged
         with resource attribute omniwatch.workspace, and persist them into
         per-workspace ClickHouse databases (omniwatch_ws_<slug>).
Inputs: omniwatch.{metrics,logs,traces}.raw Kafka topics (OTLP JSON)
Outputs: Rows in omniwatch_ws_<slug>.{metrics,logs,traces}; untagged
         signals are skipped (shared path owns them)
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

logger = logging.getLogger("omniwatch.ws-bridge")


def _env(primary: str, fallback: str, default: str) -> str:
    return os.getenv(primary, os.getenv(fallback, default))


_BOOTSTRAP = _env(
    "OMNIWATCH_KAFKA_BOOTSTRAP_SERVERS", "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
_GROUP = _env("OMNIWATCH_KAFKA_GROUP_WS_BRIDGE", "KAFKA_GROUP", "omniwatch-ws-bridge")
_TOPICS = [
    _env("OMNIWATCH_KAFKA_TOPIC_METRICS", "KAFKA_TOPIC_METRICS", "omniwatch.metrics.raw"),
    _env("OMNIWATCH_KAFKA_TOPIC_LOGS", "KAFKA_TOPIC_LOGS", "omniwatch.logs.raw"),
    _env("OMNIWATCH_KAFKA_TOPIC_TRACES", "KAFKA_TOPIC_TRACES", "omniwatch.traces.raw"),
]
_CH_HOST = _env("CLICKHOUSE_HOST", "CLICKHOUSE_HOST", "clickhouse")
_CH_PORT = int(_env("CLICKHOUSE_PORT", "CLICKHOUSE_PORT", "8123"))
_CH_DB = _env("CLICKHOUSE_DB", "CLICKHOUSE_DB", "omniwatch")
_CH_USER = _env("CLICKHOUSE_USER", "CLICKHOUSE_USER", "default")
_CH_PASSWORD = _env("CLICKHOUSE_PASSWORD", "CLICKHOUSE_PASSWORD", "")
_HEALTH_PORT = int(_env("WS_BRIDGE_HEALTH_PORT", "WS_BRIDGE_HEALTH_PORT", "8090"))
_BATCH_SIZE = 100

_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9\-_]{0,62}$")
_WS_ATTR = "omniwatch.workspace"


def _attr_list(attrs: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in attrs or []:
        key = item.get("key", "")
        val = item.get("value", {})
        for _type, text in val.items():
            if _type in ("stringValue",):
                out[key] = str(text)
            elif _type in ("intValue", "boolValue", "doubleValue"):
                out[key] = str(text)
    return out


def _ns_to_dt(ns: Any) -> datetime:
    try:
        return datetime.fromtimestamp(int(str(ns)) / 1e9, tz=timezone.utc)
    except (ValueError, TypeError, OSError):
        return datetime.now(timezone.utc)


class Bridge:
    def __init__(self) -> None:
        import clickhouse_connect
        from confluent_kafka import Consumer

        self._ch = clickhouse_connect.get_client(
            host=_CH_HOST, port=_CH_PORT, database=_CH_DB,
            username=_CH_USER, password=_CH_PASSWORD)
        self._consumer = Consumer({
            "bootstrap.servers": _BOOTSTRAP,
            "group.id": _GROUP,
            "auto.offset.reset": "latest",
        })
        self._consumer.subscribe(_TOPICS)
        self._ensured: set[str] = set()
        self._stats = {"routed": 0, "skipped": 0, "errors": 0}

    def _ensure_workspace_tables(self, slug: str) -> None:
        if slug in self._ensured:
            return
        db = f"omniwatch_ws_{slug}"
        self._ch.command(f"CREATE DATABASE IF NOT EXISTS `{db}`")
        for table in ("metrics", "logs", "traces", "anomalies", "incidents", "pending_approvals", "knowledge_base", "feature_vectors", "sessions", "users", "workspaces"):
            self._ch.command(
                f"CREATE TABLE IF NOT EXISTS `{db}`.{table} AS omniwatch.{table}")
        self._ensured.add(slug)

    def _route(self, topic: str, payload: dict[str, Any]) -> None:
        if "metrics" in topic:
            self._route_metrics(payload)
        elif "logs" in topic:
            self._route_logs(payload)
        elif "traces" in topic:
            self._route_traces(payload)

    def _workspace_of(self, attrs: dict[str, str]) -> str:
        slug = attrs.get(_WS_ATTR, "")
        if slug and _SLUG_RE.match(slug):
            return slug
        return ""

    def _route_metrics(self, payload: dict[str, Any]) -> None:
        buckets: dict[str, list] = {}
        for rm in payload.get("resourceMetrics", []):
            attrs = _attr_list(rm.get("resource", {}).get("attributes"))
            slug = self._workspace_of(attrs)
            if not slug:
                self._stats["skipped"] += 1
                continue
            entity = attrs.get("service.name", "unknown")
            rows = buckets.setdefault(slug, [])
            for sm in rm.get("scopeMetrics", []):
                for metric in sm.get("metrics", []):
                    name = metric.get("name", "")
                    for key in ("gauge", "sum"):
                        data = metric.get(key)
                        if not data:
                            continue
                        for point in data.get("dataPoints", []):
                            raw = point.get("asDouble", point.get("asInt", 0))
                            try:
                                value = float(raw)
                            except (TypeError, ValueError):
                                continue
                            tags = dict(attrs)
                            tags.update(_attr_list(point.get("attributes")))
                            rows.append((
                                entity, "SERVICE", name, value, tags, "performance",
                                _ns_to_dt(point.get("timeUnixNano", 0)),
                            ))
        for slug, rows in buckets.items():
            if not rows:
                continue
            self._ensure_workspace_tables(slug)
            db = f"omniwatch_ws_{slug}"
            self._ch.insert(
                f"`{db}`.metrics",
                rows,
                column_names=["entity_id", "entity_type", "metric_name",
                              "value", "tags", "source_type", "timestamp"],
            )
            self._stats["routed"] += len(rows)

    def _route_logs(self, payload: dict[str, Any]) -> None:
        buckets: dict[str, list] = {}
        for rl in payload.get("resourceLogs", []):
            attrs = _attr_list(rl.get("resource", {}).get("attributes"))
            slug = self._workspace_of(attrs)
            if not slug:
                self._stats["skipped"] += 1
                continue
            entity = attrs.get("service.name", "unknown")
            rows = buckets.setdefault(slug, [])
            for sl in rl.get("scopeLogs", []):
                for record in sl.get("logRecords", []):
                    body = record.get("body", {})
                    message = body.get("stringValue", "") if isinstance(body, dict) else str(body)
                    rows.append((
                        entity,
                        str(record.get("severityText", "INFO")),
                        message,
                        entity,
                        str(record.get("traceId", "")),
                        _ns_to_dt(record.get("timeUnixNano", 0)),
                    ))
        for slug, rows in buckets.items():
            if not rows:
                continue
            self._ensure_workspace_tables(slug)
            db = f"omniwatch_ws_{slug}"
            self._ch.insert(
                f"`{db}`.logs",
                rows,
                column_names=["entity_id", "log_level", "message",
                              "service_name", "trace_id", "timestamp"],
            )
            self._stats["routed"] += len(rows)

    def _route_traces(self, payload: dict[str, Any]) -> None:
        buckets: dict[str, list] = {}
        for rs in payload.get("resourceSpans", []):
            attrs = _attr_list(rs.get("resource", {}).get("attributes"))
            slug = self._workspace_of(attrs)
            if not slug:
                self._stats["skipped"] += 1
                continue
            entity = attrs.get("service.name", "unknown")
            rows = buckets.setdefault(slug, [])
            for ss in rs.get("scopeSpans", []):
                for span in ss.get("spans", []):
                    try:
                        start = int(str(span.get("startTimeUnixNano", 0)))
                        end = int(str(span.get("endTimeUnixNano", start)))
                    except (TypeError, ValueError):
                        continue
                    rows.append((
                        str(span.get("traceId", "")),
                        str(span.get("spanId", "")),
                        str(span.get("parentSpanId", "")),
                        entity,
                        str(span.get("name", "")),
                        (end - start) / 1e6,
                        _ns_to_dt(start),
                    ))
        for slug, rows in buckets.items():
            if not rows:
                continue
            self._ensure_workspace_tables(slug)
            db = f"omniwatch_ws_{slug}"
            self._ch.insert(
                f"`{db}`.traces",
                rows,
                column_names=["trace_id", "span_id", "parent_span_id",
                              "service_name", "operation", "duration_ms", "timestamp"],
            )
            self._stats["routed"] += len(rows)

    def run(self) -> None:
        logger.info(
            "ws-bridge started topics=%s group=%s", _TOPICS, _GROUP)
        batch: list[tuple[str, dict[str, Any]]] = []
        while True:
            message = self._consumer.poll(1.0)
            if message is None:
                if batch:
                    self._flush(batch)
                    batch = []
                continue
            if message.error():
                logger.warning("kafka error: %s", message.error())
                continue
            raw_value = message.value()
            topic = message.topic()
            if not raw_value or not topic:
                continue
            try:
                payload = json.loads(raw_value.decode("utf-8"))
            except (ValueError, UnicodeDecodeError, AttributeError) as exc:
                self._stats["errors"] += 1
                logger.debug("dropping undecodable message: %s", exc)
                continue
            batch.append((topic, payload))
            if len(batch) >= _BATCH_SIZE:
                self._flush(batch)
                batch = []

    def _flush(self, batch: list[tuple[str, dict[str, Any]]]) -> None:
        for topic, payload in batch:
            try:
                self._route(topic, payload)
            except Exception as exc:  # noqa: BLE001 - one bad batch never kills the loop
                self._stats["errors"] += 1
                logger.warning("bridge batch failed topic=%s: %s", topic, exc)
        logger.info("bridge stats=%s", self._stats)


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 - stdlib handler naming
        if self.path == "/health":
            body = b'{"status":"ok","service":"ws-bridge"}'
            self.send_response(200)
        else:
            body = b"{}"
            self.send_response(404)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: Any, *args: Any) -> None:
        return


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    server = HTTPServer(("0.0.0.0", _HEALTH_PORT), _HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    Bridge().run()


if __name__ == "__main__":
    main()
