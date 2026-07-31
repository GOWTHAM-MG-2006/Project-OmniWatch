"""
OmniWatch — Telemetry Ingestion: Stream Processor
Component: stream_processor.py
Phase: 2
Purpose: Consumes raw OTel telemetry from Kafka, normalizes into structured
         records, enriches with entity metadata, and publishes to downstream topics.
Inputs: omniwatch.metrics.raw, omniwatch.logs.raw, omniwatch.traces.raw,
        omniwatch.security.events (Kafka topics)
Outputs: Normalized records logged / published for Phase 3 (Entity Resolution)
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any

# Local import — falls back gracefully if confluent_kafka not installed
try:
    from kafka_bus import (
        KafkaConsumer,
        KafkaProducer,
        TOPIC_METRICS_RAW,
        TOPIC_LOGS_RAW,
        TOPIC_TRACES_RAW,
        TOPIC_SECURITY_EVENTS,
    )
except ImportError:
    KafkaConsumer = None  # type: ignore
    KafkaProducer = None  # type: ignore
    TOPIC_METRICS_RAW = "omniwatch.metrics.raw"
    TOPIC_LOGS_RAW = "omniwatch.logs.raw"
    TOPIC_TRACES_RAW = "omniwatch.traces.raw"
    TOPIC_SECURITY_EVENTS = "omniwatch.security.events"

logger = logging.getLogger("omniwatch.stream_processor")

# ---------------------------------------------------------------------------
# Entity types
# ---------------------------------------------------------------------------

class EntityType(str, Enum):
    """Known entity types for resource normalization."""
    API_NODE = "API_NODE"
    DATABASE_NODE = "DATABASE_NODE"
    CACHE_NODE = "CACHE_NODE"
    QUEUE_NODE = "QUEUE_NODE"
    WORKER_NODE = "WORKER_NODE"
    GATEWAY_NODE = "GATEWAY_NODE"
    STORAGE_NODE = "STORAGE_NODE"
    UNKNOWN = "UNKNOWN"


# Entity type hint keywords mapped from OTel resource attributes / service name
_ENTITY_TYPE_HINTS: dict[str, EntityType] = {
    "api": EntityType.API_NODE,
    "gateway": EntityType.GATEWAY_NODE,
    "nginx": EntityType.GATEWAY_NODE,
    "postgres": EntityType.DATABASE_NODE,
    "postgresql": EntityType.DATABASE_NODE,
    "mysql": EntityType.DATABASE_NODE,
    "redis": EntityType.CACHE_NODE,
    "memcached": EntityType.CACHE_NODE,
    "kafka": EntityType.QUEUE_NODE,
    "rabbitmq": EntityType.QUEUE_NODE,
    "worker": EntityType.WORKER_NODE,
    "background": EntityType.WORKER_NODE,
    "minio": EntityType.STORAGE_NODE,
    "s3": EntityType.STORAGE_NODE,
}


def infer_entity_type(service_name: str, resource_attrs: dict[str, Any] | None = None) -> EntityType:
    """Infer the entity type from service name or resource attributes."""
    name_lower = service_name.lower()
    for hint, etype in _ENTITY_TYPE_HINTS.items():
        if hint in name_lower:
            return etype
    # Check resource attributes
    if resource_attrs:
        for val in resource_attrs.values():
            if isinstance(val, str):
                val_lower = val.lower()
                for hint, etype in _ENTITY_TYPE_HINTS.items():
                    if hint in val_lower:
                        return etype
    return EntityType.UNKNOWN


# ---------------------------------------------------------------------------
# Normalized record types
# ---------------------------------------------------------------------------

@dataclass
class NormalizedMetric:
    """A single normalized metric data point."""
    entity_id: str
    entity_type: str
    metric_name: str
    value: float
    timestamp: str
    attributes: dict[str, Any] = field(default_factory=dict)
    source_topic: str = "omniwatch.metrics.raw"
    resource_attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class NormalizedLog:
    """A single normalized log record."""
    entity_id: str
    entity_type: str
    log_level: str
    body: str
    timestamp: str
    attributes: dict[str, Any] = field(default_factory=dict)
    source_topic: str = "omniwatch.logs.raw"
    resource_attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class NormalizedTrace:
    """A single normalized trace span."""
    entity_id: str
    entity_type: str
    span_id: str
    trace_id: str
    parent_span_id: str | None
    span_name: str
    duration_ns: int
    status: str
    timestamp: str
    attributes: dict[str, Any] = field(default_factory=dict)
    source_topic: str = "omniwatch.traces.raw"
    resource_attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class NormalizedSecurityEvent:
    """A single normalized security event."""
    entity_id: str
    entity_type: str
    event_type: str
    severity: str
    description: str
    timestamp: str
    source_ip: str | None = None
    attributes: dict[str, Any] = field(default_factory=dict)
    source_topic: str = "omniwatch.security.events"
    resource_attributes: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# OTel JSON parsers
# ---------------------------------------------------------------------------

def _extract_resource_attrs(payload: dict[str, Any]) -> dict[str, Any]:
    """Extract resource attributes from an OTel JSON payload."""
    resource = payload.get("resource", {})
    # OTLP JSON format: resource.attributes is a list of {key, value.{stringValue, intValue, ...}}
    attrs: dict[str, Any] = {}
    for attr in resource.get("attributes", []):
        key = attr.get("key", "")
        val = attr.get("value", {})
        # OTLP values can be stringValue, intValue, doubleValue, boolValue, etc.
        if "stringValue" in val:
            attrs[key] = val["stringValue"]
        elif "intValue" in val:
            attrs[key] = int(val["intValue"])
        elif "doubleValue" in val:
            attrs[key] = float(val["doubleValue"])
        elif "boolValue" in val:
            attrs[key] = val["boolValue"]
        else:
            attrs[key] = str(val)
    return attrs


def _extract_service_name(resource_attrs: dict[str, Any]) -> str:
    """Extract service.name from resource attributes."""
    return resource_attrs.get("service.name", resource_attrs.get("service_name", "unknown"))


_ISO_TIMESTAMP_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"


def _to_iso(timestamp_ns: int | str | None) -> str:
    """Convert nanoseconds-since-epoch to ISO 8601 string.

    OTel JSON payloads often encode ``timeUnixNano`` as a string
    to avoid JavaScript integer precision loss.
    """
    if timestamp_ns is None:
        return datetime.now(timezone.utc).strftime(_ISO_TIMESTAMP_FMT)
    try:
        if isinstance(timestamp_ns, str):
            timestamp_ns = int(timestamp_ns)
        return datetime.fromtimestamp(timestamp_ns / 1e9, tz=timezone.utc).strftime(
            _ISO_TIMESTAMP_FMT
        )
    except (OSError, ValueError, OverflowError):
        return datetime.now(timezone.utc).strftime(_ISO_TIMESTAMP_FMT)


# ---------------------------------------------------------------------------
# Metric normalization
# ---------------------------------------------------------------------------

def normalize_metric(payload: dict[str, Any]) -> list[NormalizedMetric]:
    """Normalize an OTel metrics JSON payload into NormalizedMetric records."""
    resource_attrs = _extract_resource_attrs(payload)
    service_name = _extract_service_name(resource_attrs)
    entity_type = infer_entity_type(service_name, resource_attrs).value
    results: list[NormalizedMetric] = []

    for scope_metrics in payload.get("scopeMetrics", []):
        scope = scope_metrics.get("scope", {})
        scope_name = scope.get("name", "")

        for metric in scope_metrics.get("metrics", []):
            metric_name = metric.get("name", "unknown_metric")
            unit = metric.get("unit", "")

            # Gauge
            for dp in metric.get("gauge", {}).get("dataPoints", []):
                results.append(_metric_dp(dp, metric_name, unit, service_name, entity_type, resource_attrs))

            # Sum
            for dp in metric.get("sum", {}).get("dataPoints", []):
                results.append(_metric_dp(dp, metric_name, unit, service_name, entity_type, resource_attrs))

            # Histogram
            for dp in metric.get("histogram", {}).get("dataPoints", []):
                count = dp.get("count", 0)
                _sum = dp.get("sum", 0.0)
                results.append(NormalizedMetric(
                    entity_id=service_name,
                    entity_type=entity_type,
                    metric_name=f"{metric_name}_sum",
                    value=float(_sum),
                    timestamp=_to_iso(dp.get("timeUnixNano")),
                    attributes={"unit": unit, "count": count},
                    resource_attributes=resource_attrs,
                ))
                # min/max from explicit bounds if available
                for i, b in enumerate(dp.get("explicitBounds", [])):
                    bucket_count = dp.get("bucketCounts", [])[i] if i < len(dp.get("bucketCounts", [])) else 0
                    results.append(NormalizedMetric(
                        entity_id=service_name,
                        entity_type=entity_type,
                        metric_name=f"{metric_name}_bucket",
                        value=float(bucket_count),
                        timestamp=_to_iso(dp.get("timeUnixNano")),
                        attributes={"unit": unit, "bound": b, "count": bucket_count},
                        resource_attributes=resource_attrs,
                    ))

    return results


def _metric_dp(
    dp: dict[str, Any],
    metric_name: str,
    unit: str,
    service_name: str,
    entity_type: str,
    resource_attrs: dict[str, Any],
) -> NormalizedMetric:
    """Convert a single metric data point."""
    value = 0.0
    if "asDouble" in dp:
        raw = dp["asDouble"]
        value = float(raw)
    elif "asInt" in dp:
        raw = dp["asInt"]
        # OTel JSON encodes int64 as string to avoid JS precision loss
        value = float(int(raw))

    attrs: dict[str, Any] = {"unit": unit}
    for attr in dp.get("attributes", []):
        attrs[attr.get("key", "")] = attr.get("value", {}).get("stringValue", str(attr.get("value", {})))

    return NormalizedMetric(
        entity_id=service_name,
        entity_type=entity_type,
        metric_name=metric_name,
        value=value,
        timestamp=_to_iso(dp.get("timeUnixNano")),
        attributes=attrs,
        resource_attributes=resource_attrs,
    )


# ---------------------------------------------------------------------------
# Log normalization
# ---------------------------------------------------------------------------

_LOG_LEVEL_MAP: dict[int, str] = {
    1: "TRACE", 2: "TRACE", 3: "DEBUG", 4: "DEBUG",
    5: "INFO", 6: "INFO", 7: "WARN", 8: "WARN",
    9: "ERROR", 10: "ERROR", 11: "FATAL", 12: "FATAL",
}


def normalize_log(payload: dict[str, Any]) -> list[NormalizedLog]:
    """Normalize an OTel logs JSON payload into NormalizedLog records."""
    resource_attrs = _extract_resource_attrs(payload)
    service_name = _extract_service_name(resource_attrs)
    entity_type = infer_entity_type(service_name, resource_attrs).value
    results: list[NormalizedLog] = []

    for scope_logs in payload.get("scopeLogs", []):
        for log_record in scope_logs.get("logRecords", []):
            severity_number = log_record.get("severityNumber", 0)
            log_level = _LOG_LEVEL_MAP.get(severity_number, "UNKNOWN")

            attrs: dict[str, Any] = {}
            for attr in log_record.get("attributes", []):
                attrs[attr.get("key", "")] = attr.get("value", {}).get("stringValue", str(attr.get("value", {})))

            # Falls back to body.stringValue or body as string
            body = log_record.get("body", {}).get("stringValue", str(log_record.get("body", "")))

            results.append(NormalizedLog(
                entity_id=service_name,
                entity_type=entity_type,
                log_level=log_level,
                body=body,
                timestamp=_to_iso(log_record.get("timeUnixNano")),
                attributes=attrs,
                resource_attributes=resource_attrs,
            ))

    return results


# ---------------------------------------------------------------------------
# Trace normalization
# ---------------------------------------------------------------------------

_SPAN_STATUS_MAP: dict[int, str] = {
    0: "UNSET", 1: "OK", 2: "ERROR",
}


def normalize_trace(payload: dict[str, Any]) -> list[NormalizedTrace]:
    """Normalize an OTel traces JSON payload into NormalizedTrace records."""
    resource_attrs = _extract_resource_attrs(payload)
    service_name = _extract_service_name(resource_attrs)
    entity_type = infer_entity_type(service_name, resource_attrs).value
    results: list[NormalizedTrace] = []

    for scope_spans in payload.get("scopeSpans", []):
        for span in scope_spans.get("spans", []):
            attrs: dict[str, Any] = {}
            for attr in span.get("attributes", []):
                attrs[attr.get("key", "")] = attr.get("value", {}).get("stringValue", str(attr.get("value", {})))

            status_code = span.get("status", {}).get("code", 0)

            results.append(NormalizedTrace(
                entity_id=service_name,
                entity_type=entity_type,
                span_id=span.get("spanId", ""),
                trace_id=span.get("traceId", ""),
                parent_span_id=span.get("parentSpanId"),
                span_name=span.get("name", ""),
                duration_ns=int(span.get("endTimeUnixNano", 0)) - int(span.get("startTimeUnixNano", 0)),
                status=_SPAN_STATUS_MAP.get(status_code, "UNSET"),
                timestamp=_to_iso(span.get("startTimeUnixNano")),
                attributes=attrs,
                resource_attributes=resource_attrs,
            ))

    return results


# ---------------------------------------------------------------------------
# Security event normalization
# ---------------------------------------------------------------------------

def normalize_security_event(payload: dict[str, Any]) -> list[NormalizedSecurityEvent]:
    """Normalize a security event payload into NormalizedSecurityEvent records.

    Security events are custom JSON (not OTLP format). Expected shape:
    {
        "entity_id": "...", "event_type": "BRUTE_FORCE",
        "severity": "HIGH", "description": "...",
        "source_ip": "...", "timestamp": "...",
        "attributes": {...}
    }
    """
    entity_id = payload.get("entity_id", payload.get("service_name", "unknown"))
    entity_type = infer_entity_type(entity_id).value
    timestamp = payload.get("timestamp", datetime.now(timezone.utc).strftime(_ISO_TIMESTAMP_FMT))

    return [NormalizedSecurityEvent(
        entity_id=entity_id,
        entity_type=entity_type,
        event_type=payload.get("event_type", payload.get("scenario", "unknown")),
        severity=payload.get("severity", "UNKNOWN"),
        description=payload.get("description", payload.get("detail", str(payload))),
        timestamp=timestamp,
        source_ip=payload.get("source_ip"),
        attributes=payload.get("attributes", {}),
    )]


# ---------------------------------------------------------------------------
# Router — dispatch payload to correct normalizer based on content shape
# ---------------------------------------------------------------------------

_NORMALIZED_OUTPUT_TOPIC = os.getenv("NORMALIZED_OUTPUT_TOPIC", "omniwatch.metrics.normalized")
_NORMALIZED_LOG_TOPIC = os.getenv("NORMALIZED_LOG_TOPIC", "omniwatch.logs.normalized")
_NORMALIZED_TRACE_TOPIC = os.getenv("NORMALIZED_TRACE_TOPIC", "omniwatch.traces.normalized")
_NORMALIZED_SECURITY_TOPIC = os.getenv("NORMALIZED_SECURITY_TOPIC", "omniwatch.security.normalized")


def detect_source_topic(payload: dict[str, Any]) -> str | None:
    """Heuristically detect which OmniWatch topic a payload originated from."""
    if "scopeMetrics" in payload or "resourceMetrics" in payload:
        return TOPIC_METRICS_RAW
    if "scopeLogs" in payload or "resourceLogs" in payload:
        return TOPIC_LOGS_RAW
    if "scopeSpans" in payload or "resourceSpans" in payload:
        return TOPIC_TRACES_RAW
    if "event_type" in payload or "scenario" in payload:
        if payload.get("event_type") in ("BRUTE_FORCE", "CONFIG_DRIFT", "DATA_EXFILTRATION", "PRIVILEGE_ESCALATION"):
            return TOPIC_SECURITY_EVENTS
        # Generic security heuristics
        if payload.get("severity") in ("HIGH", "CRITICAL") or "attack" in str(payload).lower():
            return TOPIC_SECURITY_EVENTS
    return None


def process_payload(payload: dict[str, Any], source_topic: str | None = None) -> list[dict[str, Any]]:
    """Process a single payload and return normalized records as dicts.

    Args:
        payload: The raw JSON message value.
        source_topic: Explicit source topic. Auto-detected if None.

    Returns:
        List of normalized record dicts with a '_type' field indicating record type.
    """
    if source_topic is None:
        source_topic = detect_source_topic(payload) or "unknown"

    if source_topic == TOPIC_METRICS_RAW:
        return [asdict(r) for r in normalize_metric(payload)]
    elif source_topic == TOPIC_LOGS_RAW:
        return [asdict(r) for r in normalize_log(payload)]
    elif source_topic == TOPIC_TRACES_RAW:
        return [asdict(r) for r in normalize_trace(payload)]
    elif source_topic == TOPIC_SECURITY_EVENTS:
        return [asdict(r) for r in normalize_security_event(payload)]

    # Fallback: try all normalizers and take the one that produces results
    results = normalize_metric(payload)
    if results:
        return [asdict(r) for r in results]
    results = normalize_log(payload)
    if results:
        return [asdict(r) for r in results]
    results = normalize_trace(payload)
    if results:
        return [asdict(r) for r in results]
    results = normalize_security_event(payload)
    if results:
        return [asdict(r) for r in results]

    logger.warning("[stream_processor] unrecognized payload shape: keys=%s", list(payload.keys()))
    return []


# ---------------------------------------------------------------------------
# StreamProcessor — continuous processing loop
# ---------------------------------------------------------------------------

class StreamProcessor:
    """Continuous stream processor that reads from Kafka, normalizes, and logs.

    Usage::

        processor = StreamProcessor()
        processor.run(batch_size=10, poll_timeout=5.0)
    """

    INPUT_TOPICS = [
        TOPIC_METRICS_RAW,
        TOPIC_LOGS_RAW,
        TOPIC_TRACES_RAW,
        TOPIC_SECURITY_EVENTS,
    ]

    def __init__(
        self,
        group_id: str = "stream-processor",
        bootstrap_servers: str | None = None,
    ) -> None:
        self._group_id = group_id
        self._bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self._consumer: Any = None
        self._running = False
        self._shutdown_event = threading.Event()
        self._processed_count = 0
        self._error_count = 0

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum: int, _frame: Any) -> None:
        logger.info("[stream_processor] received signal %d, shutting down...", signum)
        self._shutdown_event.set()

    def process_message(self, msg: dict[str, Any]) -> list[dict[str, Any]]:
        """Process a single message from Kafka."""
        value = msg.get("value")
        topic = msg.get("topic", "unknown")
        if value is None:
            return []
        try:
            normalized = process_payload(value, source_topic=topic)
            return normalized
        except Exception as exc:
            logger.error("[stream_processor] processing error: topic=%s error=%s", topic, exc)
            self._error_count += 1
            return []

    def run(
        self,
        batch_size: int = 50,
        poll_timeout: float = 5.0,
        max_messages: int = 0,
    ) -> None:
        """Run the stream processor loop.

        Args:
            batch_size: Max messages per poll iteration.
            poll_timeout: Seconds to wait for messages each poll.
            max_messages: If > 0, stop after processing this many messages.
        """
        if KafkaConsumer is None:
            logger.error("[stream_processor] confluent_kafka not installed — cannot run")
            print("[FAIL] confluent_kafka is required. Install with: pip install confluent-kafka")
            return

        logger.info(
            "[stream_processor] starting — topics=%s group=%s servers=%s",
            self.INPUT_TOPICS,
            self._group_id,
            self._bootstrap_servers,
        )

        self._consumer = KafkaConsumer(
            topics=self.INPUT_TOPICS,
            group_id=self._group_id,
            bootstrap_servers=self._bootstrap_servers,
        )
        try:
            self._consumer.start()
        except Exception as exc:
            logger.error("[stream_processor] failed to start consumer: %s", exc)
            print(f"[FAIL] Consumer start failed: {exc}")
            return

        self._running = True
        self._shutdown_event.clear()
        start_time = time.time()

        try:
            while self._running and not self._shutdown_event.is_set():
                messages = self._consumer.messages(
                    timeout=poll_timeout,
                    max_messages=batch_size,
                )
                if not messages:
                    continue

                for msg in messages:
                    normalized = self.process_message(msg)
                    self._processed_count += len(normalized)
                    for record in normalized:
                        rtype = record.get("entity_type", "?")
                        metric = record.get("metric_name", record.get("event_type", "?"))
                        logger.info(
                            "[stream_processor] normalized: type=%s entity=%s metric=%s",
                            rtype,
                            record.get("entity_id", "?"),
                            metric,
                        )

                    if max_messages > 0 and self._processed_count >= max_messages:
                        logger.info(
                            "[stream_processor] reached max_messages=%d", max_messages
                        )
                        self._running = False
                        break

        finally:
            self._consumer.stop()
            elapsed = time.time() - start_time
            logger.info(
                "[stream_processor] stopped — processed=%d errors=%d elapsed=%.1fs",
                self._processed_count,
                self._error_count,
                elapsed,
            )
            print(
                f"\n[OK] Stream processor stopped."
                f"\n     Processed: {self._processed_count}"
                f"\n     Errors:    {self._error_count}"
                f"\n     Elapsed:   {elapsed:.1f}s"
            )

    @property
    def processed_count(self) -> int:
        return self._processed_count

    @property
    def error_count(self) -> int:
        return self._error_count


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def cli_run() -> None:
    """Run the stream processor in continuous mode."""
    import argparse

    parser = argparse.ArgumentParser(
        description="OmniWatch Stream Processor — normalizes OTel telemetry from Kafka."
    )
    parser.add_argument(
        "--poll-timeout",
        type=float,
        default=5.0,
        help="Seconds to wait for messages per poll (default: 5.0)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Max messages per poll (default: 50)",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after N messages (0 = continuous, default: 0)",
    )
    parser.add_argument(
        "--group-id",
        default="stream-processor",
        help="Kafka consumer group ID (default: stream-processor)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args(sys.argv[2:])

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    processor = StreamProcessor(group_id=args.group_id)
    processor.run(
        batch_size=args.batch_size,
        poll_timeout=args.poll_timeout,
        max_messages=args.max_messages,
    )


def cli_parse() -> None:
    """CLI entry point for parsing a single JSON payload (for testing)."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Parse a single OTel JSON payload file and print normalized records."
    )
    parser.add_argument("file", help="Path to OTel JSON payload file")
    parser.add_argument("--topic", default=None, help="Source topic hint")
    args = parser.parse_args(sys.argv[2:])

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    with open(args.file, "r", encoding="utf-8") as fh:
        payload = json.load(fh)

    records = process_payload(payload, source_topic=args.topic)
    print(json.dumps(records, indent=2, default=str))
    print(f"\n--- {len(records)} normalized records ---")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == "run":
            cli_run()
        elif command == "parse":
            cli_parse()
        else:
            print(f"Unknown command: {command}")
            print("Usage: python stream_processor.py [run | parse <file>]")
            sys.exit(1)
    else:
        print("Usage: python stream_processor.py [run | parse <file>]")
        sys.exit(1)
