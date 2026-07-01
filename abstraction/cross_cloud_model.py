"""
OmniWatch — Cloud Abstraction Layer
Component: Cross-Cloud Model
Phase: 3
Purpose: Unified schema for multi-cloud resources — defines the common data model
Inputs: Normalized and mapped events from previous components
Outputs: Unified event objects conforming to OmniWatch data contracts
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from dataclasses import dataclass, field, asdict

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")


# ---------------------------------------------------------------------------
# Unified data models (matching AGENTS.md data contracts)
# ---------------------------------------------------------------------------

@dataclass
class UnifiedMetric:
    """Unified metric event conforming to OmniWatch schema."""
    entity_id: str
    entity_type: str
    metric_name: str
    metric_value: float
    cloud_provider: str
    timestamp: str
    source: str = "simulation"
    labels: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class UnifiedLog:
    """Unified log event conforming to OmniWatch schema."""
    entity_id: str
    entity_type: str
    log_level: str
    message: str
    cloud_provider: str
    timestamp: str
    source: str = "simulation"
    labels: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class UnifiedTrace:
    """Unified trace event conforming to OmniWatch schema."""
    entity_id: str
    entity_type: str
    trace_id: str
    span_id: str
    operation_name: str
    duration_ms: float
    cloud_provider: str
    timestamp: str
    status: str = "ok"
    source: str = "simulation"

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class UnifiedSecurityEvent:
    """Unified security event conforming to OmniWatch schema."""
    event_type: str
    entity_id: str
    entity_type: str
    severity: str
    source_ip: Optional[str] = None
    timestamp: str = ""
    source_type: str = "security"
    evidence_logs: list = field(default_factory=list)
    recommended_action: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Cross-cloud model builder
# ---------------------------------------------------------------------------

class CrossCloudModel:
    """
    Builds unified event objects from raw or normalized telemetry.

    Usage:
        model = CrossCloudModel()
        metric = model.build_metric(raw_event)
        log = model.build_log(raw_event)
    """

    def build_metric(self, event: dict) -> UnifiedMetric:
        """Build a UnifiedMetric from a raw or normalized event."""
        return UnifiedMetric(
            entity_id=event.get("entity_id", "unknown"),
            entity_type=event.get("entity_type", "UNKNOWN_NODE"),
            metric_name=event.get("metric_name", event.get("name", "unknown")),
            metric_value=float(event.get("metric_value", event.get("value", 0.0))),
            cloud_provider=event.get("cloud_provider", "unknown"),
            timestamp=event.get("timestamp", datetime.now(timezone.utc).isoformat()),
            source=event.get("source", "simulation"),
            labels=event.get("labels", {}),
        )

    def build_log(self, event: dict) -> UnifiedLog:
        """Build a UnifiedLog from a raw or normalized event."""
        return UnifiedLog(
            entity_id=event.get("entity_id", "unknown"),
            entity_type=event.get("entity_type", "UNKNOWN_NODE"),
            log_level=event.get("log_level", event.get("level", "info")),
            message=event.get("message", ""),
            cloud_provider=event.get("cloud_provider", "unknown"),
            timestamp=event.get("timestamp", datetime.now(timezone.utc).isoformat()),
            source=event.get("source", "simulation"),
            labels=event.get("labels", {}),
        )

    def build_trace(self, event: dict) -> UnifiedTrace:
        """Build a UnifiedTrace from a raw or normalized event."""
        return UnifiedTrace(
            entity_id=event.get("entity_id", "unknown"),
            entity_type=event.get("entity_type", "UNKNOWN_NODE"),
            trace_id=event.get("trace_id", ""),
            span_id=event.get("span_id", ""),
            operation_name=event.get("operation_name", event.get("operation", "")),
            duration_ms=float(event.get("duration_ms", 0.0)),
            cloud_provider=event.get("cloud_provider", "unknown"),
            timestamp=event.get("timestamp", datetime.now(timezone.utc).isoformat()),
            status=event.get("status", "ok"),
            source=event.get("source", "simulation"),
        )

    def build_security_event(self, event: dict) -> UnifiedSecurityEvent:
        """Build a UnifiedSecurityEvent from a raw or normalized event."""
        return UnifiedSecurityEvent(
            event_type=event.get("event_type", "UNKNOWN"),
            entity_id=event.get("entity_id", "unknown"),
            entity_type=event.get("entity_type", "UNKNOWN_NODE"),
            severity=event.get("severity", "LOW"),
            source_ip=event.get("source_ip"),
            timestamp=event.get("timestamp", datetime.now(timezone.utc).isoformat()),
            source_type=event.get("source_type", "security"),
            evidence_logs=event.get("evidence_logs", []),
            recommended_action=event.get("recommended_action", ""),
        )


# ---------------------------------------------------------------------------
# CLI interface
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="OmniWatch Cross-Cloud Model")
    subparsers = parser.add_subparsers(dest="command")

    # build command
    build_parser = subparsers.add_parser("build", help="Build a unified event")
    build_parser.add_argument("--type", choices=["metric", "log", "trace", "security"], required=True)
    build_parser.add_argument("--entity", required=True, help="Entity ID")
    build_parser.add_argument("--name", default="test_metric", help="Metric/event name")
    build_parser.add_argument("--value", type=float, default=42.0, help="Value")

    args = parser.parse_args()
    model = CrossCloudModel()

    if args.command == "build":
        event = {
            "entity_id": args.entity,
            "entity_type": "API_NODE",
            "cloud_provider": "simulated-aws",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if args.type == "metric":
            event["metric_name"] = args.name
            event["metric_value"] = args.value
            result = model.build_metric(event)
        elif args.type == "log":
            event["log_level"] = "info"
            event["message"] = f"Test log for {args.entity}"
            result = model.build_log(event)
        elif args.type == "trace":
            event["trace_id"] = "abc123"
            event["span_id"] = "def456"
            event["operation_name"] = args.name
            event["duration_ms"] = args.value
            result = model.build_trace(event)
        elif args.type == "security":
            event["event_type"] = "BRUTE_FORCE_ATTEMPT"
            event["severity"] = "HIGH"
            result = model.build_security_event(event)

        print(json.dumps(result.to_dict(), indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
