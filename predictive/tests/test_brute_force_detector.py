"""
OmniWatch — Predictive Intelligence Layer
Component: Brute Force Detector Tests
Phase: 6
Purpose: Unit tests for BruteForceDetector (windowed auth failure counting)
Inputs: Synthetic security event dicts
Outputs: pytest pass/fail
"""

from __future__ import annotations

import time
from typing import Any, Dict
from unittest.mock import patch

import pytest

from predictive.security.brute_force_detector import (
    BruteForceDetector,
    _load_brute_force_rule,
)


# ─── Helpers ──────────────────────────────────────────────────────────────── #


def _make_auth_failure_event(
    source_ip: str = "192.168.1.100",
    **overrides: Any,
) -> Dict[str, Any]:
    """Build a minimal auth-failure security event dict."""
    base: Dict[str, Any] = {
        "entity_id": "web-server-01",
        "source_ip": source_ip,
        "event_type": "auth_failure",
        "message": "Failed password for root",
        "timestamp": "2026-08-02T12:00:00Z",
    }
    base.update(overrides)
    return base


def _make_non_auth_event(**overrides: Any) -> Dict[str, Any]:
    """Build a security event that is NOT an auth failure."""
    base: Dict[str, Any] = {
        "entity_id": "web-server-01",
        "source_ip": "10.0.0.1",
        "event_type": "port_scan",
        "message": "SYN flood detected",
        "timestamp": "2026-08-02T12:00:00Z",
    }
    base.update(overrides)
    return base


# ─── Tests: _load_brute_force_rule ────────────────────────────────────────── #


class TestLoadBruteForceRule:
    def test_loads_from_yaml(self, tmp_path: Any) -> None:
        """Verify rule is loaded from a YAML file."""
        yaml_content = (
            "attack_types:\n"
            "  brute_force:\n"
            "    failures_threshold: 5\n"
            "    window_minutes: 3\n"
            "    severity: CRITICAL\n"
        )
        rules_file = tmp_path / "security_rules.yaml"
        rules_file.write_text(yaml_content, encoding="utf-8")

        rule = _load_brute_force_rule(rules_file)
        assert rule["failures_threshold"] == 5
        assert rule["window_minutes"] == 3
        assert rule["severity"] == "CRITICAL"

    def test_missing_file_returns_defaults(self, tmp_path: Any) -> None:
        """Verify defaults are used when YAML file doesn't exist."""
        rule = _load_brute_force_rule(tmp_path / "nonexistent.yaml")
        assert rule["failures_threshold"] == 10
        assert rule["window_minutes"] == 5
        assert rule["severity"] == "HIGH"

    def test_missing_brute_force_key_returns_defaults(self, tmp_path: Any) -> None:
        """Verify defaults when brute_force key is absent from YAML."""
        yaml_content = "attack_types:\n  config_drift:\n    trigger: test\n"
        rules_file = tmp_path / "security_rules.yaml"
        rules_file.write_text(yaml_content, encoding="utf-8")

        rule = _load_brute_force_rule(rules_file)
        assert rule["failures_threshold"] == 10


# ─── Tests: BruteForceDetector ────────────────────────────────────────────── #


class TestBruteForceDetector:
    """Core detection tests."""

    def test_below_threshold_returns_none(self) -> None:
        """5 failures from same IP should NOT trigger detection (threshold=10)."""
        det = BruteForceDetector(failures_threshold=10, window_minutes=5)
        result: dict[str, Any] | None = None
        for _ in range(5):
            result = det.detect(_make_auth_failure_event())
        assert result is None

    def test_at_threshold_returns_signal(self) -> None:
        """10 failures from same IP should trigger BRUTE_FORCE_ATTEMPT."""
        det = BruteForceDetector(failures_threshold=10, window_minutes=5)
        result = None
        for _ in range(10):
            result = det.detect(_make_auth_failure_event())
        assert result is not None
        assert result["attack_type"] == "BRUTE_FORCE_ATTEMPT"
        assert result["severity"] == "HIGH"
        assert result["source_ip"] == "192.168.1.100"

    def test_above_threshold_returns_signal(self) -> None:
        """15 failures should trigger with confidence=min(15*2, 100)=30."""
        det = BruteForceDetector(failures_threshold=10, window_minutes=5)
        result = None
        for _ in range(15):
            result = det.detect(_make_auth_failure_event())
        assert result is not None
        assert result["confidence"] == 30.0  # 15 * 2

    def test_confidence_cap_at_100(self) -> None:
        """Confidence should cap at 100 even with 60+ failures."""
        det = BruteForceDetector(failures_threshold=10, window_minutes=5)
        result = None
        for _ in range(60):
            result = det.detect(_make_auth_failure_event())
        assert result is not None
        assert result["confidence"] == 100.0

    def test_confidence_below_cap(self) -> None:
        """Confidence = count*2 when count < 50."""
        det = BruteForceDetector(failures_threshold=5, window_minutes=5)
        result = None
        for _ in range(7):
            result = det.detect(_make_auth_failure_event())
        assert result is not None
        assert result["confidence"] == 14.0  # 7 * 2

    def test_window_expiry_resets_count(self) -> None:
        """Events outside the window should be pruned and not counted."""
        with patch("predictive.security.brute_force_detector.time") as mock_time:
            base_time = 1000.0
            mock_time.time.return_value = base_time

            det = BruteForceDetector(failures_threshold=10, window_minutes=1)

            # Send 8 failures at t=1000
            for _ in range(8):
                det.detect(_make_auth_failure_event())

            # Now advance past the 60-second window
            mock_time.time.return_value = base_time + 61.0

            # Send 2 more — should NOT trigger (only 2 in window, 8 expired)
            result: dict[str, Any] | None = None
            for _ in range(2):
                result = det.detect(_make_auth_failure_event())

        assert result is None

    def test_separate_ips_tracked_independently(self) -> None:
        """Different source IPs should have independent counts."""
        det = BruteForceDetector(failures_threshold=10, window_minutes=5)

        # 9 from IP A — below threshold
        result: dict[str, Any] | None = None
        for _ in range(9):
            result = det.detect(_make_auth_failure_event(source_ip="10.0.0.1"))
        assert result is None

        # 9 from IP B — also below threshold
        for _ in range(9):
            result = det.detect(_make_auth_failure_event(source_ip="10.0.0.2"))
        assert result is None

        # 1 more from IP A — now at 10, triggers
        result = det.detect(_make_auth_failure_event(source_ip="10.0.0.1"))
        assert result is not None
        assert result["source_ip"] == "10.0.0.1"

    def test_non_auth_event_ignored(self) -> None:
        """Non-auth-failure events should be ignored."""
        det = BruteForceDetector(failures_threshold=10, window_minutes=5)
        result: dict[str, Any] | None = None
        for _ in range(20):
            result = det.detect(_make_non_auth_event())
        assert result is None

    def test_evidence_logs_populated(self) -> None:
        """Evidence logs should contain up to 5 recent failure entries."""
        det = BruteForceDetector(failures_threshold=10, window_minutes=5)
        result = None
        for _ in range(12):
            result = det.detect(_make_auth_failure_event())
        assert result is not None
        assert len(result["evidence_logs"]) == 5  # last 5 only
        assert all("Auth failure #" in log for log in result["evidence_logs"])
        assert all("192.168.1.100" in log for log in result["evidence_logs"])

    def test_recommended_action_contains_ip(self) -> None:
        """Recommended action should mention blocking the source IP."""
        det = BruteForceDetector(failures_threshold=10, window_minutes=5)
        result = None
        for _ in range(10):
            result = det.detect(_make_auth_failure_event(source_ip="1.2.3.4"))
        assert result is not None
        assert "1.2.3.4" in result["recommended_action"]
        assert "Block" in result["recommended_action"]

    def test_entity_id_format(self) -> None:
        """entity_id should follow the pattern brute-force-{source_ip}."""
        det = BruteForceDetector(failures_threshold=10, window_minutes=5)
        result = None
        for _ in range(10):
            result = det.detect(_make_auth_failure_event(source_ip="5.6.7.8"))
        assert result is not None
        assert result["entity_id"] == "brute-force-5.6.7.8"

    def test_timestamp_is_iso_format(self) -> None:
        """Timestamp should be a valid ISO 8601 string."""
        det = BruteForceDetector(failures_threshold=10, window_minutes=5)
        result = None
        for _ in range(10):
            result = det.detect(_make_auth_failure_event())
        assert result is not None
        # Should contain T and Z (or +00:00) indicating ISO format
        ts = result["timestamp"]
        assert "T" in ts

    def test_custom_threshold_and_window(self) -> None:
        """Custom thresholds should be respected."""
        det = BruteForceDetector(failures_threshold=3, window_minutes=2)
        result = None
        for _ in range(3):
            result = det.detect(_make_auth_failure_event())
        assert result is not None
        assert result["attack_type"] == "BRUTE_FORCE_ATTEMPT"

    def test_auth_failure_via_message_field(self) -> None:
        """Events detected via 'message' containing 'failed_password'."""
        det = BruteForceDetector(failures_threshold=3, window_minutes=5)
        event = {
            "source_ip": "10.10.10.10",
            "message": "failed_password for admin",
        }
        result = None
        for _ in range(3):
            result = det.detect(event)
        assert result is not None

    def test_auth_failure_via_attack_type_field(self) -> None:
        """Events detected via 'attack_type' containing 'auth'."""
        det = BruteForceDetector(failures_threshold=3, window_minutes=5)
        event = {
            "source_ip": "10.10.10.10",
            "attack_type": "auth_brute_force",
        }
        result = None
        for _ in range(3):
            result = det.detect(event)
        assert result is not None

    def test_no_source_ip_defaults_to_unknown(self) -> None:
        """Missing source_ip should default to 'unknown'."""
        det = BruteForceDetector(failures_threshold=3, window_minutes=5)
        event = {
            "event_type": "auth_failure",
            "message": "Failed login attempt",
        }
        result = None
        for _ in range(3):
            result = det.detect(event)
        assert result is not None
        assert result["source_ip"] == "unknown"
        assert result["entity_id"] == "brute-force-unknown"

    def test_properties(self) -> None:
        """Properties should return configured values."""
        det = BruteForceDetector(failures_threshold=7, window_minutes=3)
        assert det.failures_threshold == 7
        assert det.window_seconds == 180.0

    def test_loads_from_yaml_by_default(self, tmp_path: Any) -> None:
        """Detector should read thresholds from security_rules.yaml."""
        yaml_content = (
            "attack_types:\n"
            "  brute_force:\n"
            "    failures_threshold: 4\n"
            "    window_minutes: 2\n"
            "    severity: CRITICAL\n"
        )
        rules_file = tmp_path / "security_rules.yaml"
        rules_file.write_text(yaml_content, encoding="utf-8")

        det = BruteForceDetector(rules_path=rules_file)
        assert det.failures_threshold == 4
        assert det.window_seconds == 120.0

        result = None
        for _ in range(4):
            result = det.detect(_make_auth_failure_event())
        assert result is not None
        assert result["severity"] == "CRITICAL"
