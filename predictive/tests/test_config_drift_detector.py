"""
OmniWatch — Predictive Intelligence Layer
Component: Config Drift Detector Tests
Phase: 6
Purpose: Unit tests for ConfigDriftDetector (unauthorized config change detection)
Inputs: Synthetic security event dicts
Outputs: pytest pass/fail
"""

from __future__ import annotations

import pytest

from predictive.security.config_drift_detector import (
    ConfigDriftDetector,
    _is_approved,
    _load_rules,
    _matches_config_drift,
)


# ─── Helpers ──────────────────────────────────────────────────────────────── #


def _make_config_drift_event(
    *,
    entity_id: str = "api-gateway",
    description: str = "config_file_changed /etc/nginx/nginx.conf",
    source_ip: str = "10.0.0.5",
    change_id: str | None = None,
    **extra: object,
) -> dict:
    """Build a typical config-drift security event."""
    event: dict = {
        "entity_id": entity_id,
        "description": description,
        "source_ip": source_ip,
        "timestamp": "2026-08-02T12:00:00Z",
    }
    if change_id is not None:
        event["change_id"] = change_id
    event.update(extra)
    return event


def _make_normal_event() -> dict:
    """Build a non-config-drift security event."""
    return {
        "entity_id": "user-service",
        "attack_type": "BRUTE_FORCE",
        "description": "Multiple failed login attempts",
        "source_ip": "192.168.1.100",
        "timestamp": "2026-08-02T12:00:00Z",
    }


# ─── Tests: _matches_config_drift ────────────────────────────────────────── #


class TestMatchesConfigDrift:
    """Pattern-matching helper tests."""

    def test_config_file_changed_in_description(self) -> None:
        assert _matches_config_drift({"description": "config_file_changed nginx.conf"}) is True

    def test_config_drift_in_attack_type(self) -> None:
        assert _matches_config_drift({"attack_type": "CONFIG_DRIFT"}) is True

    def test_config_drift_case_insensitive(self) -> None:
        assert _matches_config_drift({"description": "Config Drift Detected"}) is True

    def test_config_drift_in_attributes(self) -> None:
        event = {"attributes": {"event_type": "config_file_changed"}}
        assert _matches_config_drift(event) is True

    def test_unauthorized_config_change_pattern(self) -> None:
        assert _matches_config_drift({"type": "unauthorized_config_change"}) is True

    def test_config_drift_in_event_field(self) -> None:
        assert _matches_config_drift({"event": "CONFIG_DRIFT on db-host"}) is True

    def test_normal_event_no_match(self) -> None:
        assert _matches_config_drift(_make_normal_event()) is False

    def test_empty_event_no_match(self) -> None:
        assert _matches_config_drift({}) is False

    def test_non_string_field_ignored(self) -> None:
        assert _matches_config_drift({"description": 12345}) is False


# ─── Tests: _is_approved ─────────────────────────────────────────────────── #


class TestIsApproved:
    """Approved-change lookup tests."""

    def test_change_id_approved(self) -> None:
        event = {"change_id": "CHG-001"}
        assert _is_approved(event, {"CHG-001"}) is True

    def test_file_path_approved(self) -> None:
        event = {"file": "/etc/nginx/nginx.conf"}
        assert _is_approved(event, {"/etc/nginx/nginx.conf"}) is True

    def test_description_approved(self) -> None:
        event = {"description": "Scheduled maintenance window"}
        assert _is_approved(event, {"Scheduled maintenance window"}) is True

    def test_not_approved(self) -> None:
        event = {"change_id": "CHG-999"}
        assert _is_approved(event, {"CHG-001"}) is False

    def test_empty_approved_list(self) -> None:
        event = {"change_id": "CHG-001"}
        assert _is_approved(event, set()) is False

    def test_none_approved_list(self) -> None:
        event = {"change_id": "CHG-001"}
        assert _is_approved(event, None) is False

    def test_attribute_approved(self) -> None:
        event = {"attributes": {"change_id": "CHG-002"}}
        assert _is_approved(event, {"CHG-002"}) is True


# ─── Tests: _load_rules ──────────────────────────────────────────────────── #


class TestLoadRules:
    """YAML config loading tests."""

    def test_load_default_rules(self) -> None:
        rules = _load_rules()
        assert "severity" in rules or "trigger" in rules  # has at least some content

    def test_missing_file_returns_fallback(self, tmp_path) -> None:
        fake_path = tmp_path / "nonexistent.yaml"
        rules = _load_rules(fake_path)
        assert rules["trigger"] == "unauthorized_config_change"
        assert rules["severity"] == "CRITICAL"

    def test_malformed_yaml_returns_fallback(self, tmp_path) -> None:
        bad_file = tmp_path / "bad.yaml"
        bad_file.write_text("{{{{not valid yaml", encoding="utf-8")
        rules = _load_rules(bad_file)
        assert rules["trigger"] == "unauthorized_config_change"


# ─── Tests: ConfigDriftDetector.detect() ─────────────────────────────────── #


class TestConfigDriftDetector:
    """Core detection tests — all mocked, no infrastructure needed."""

    def test_config_drift_detected(self) -> None:
        """config_file_changed event → SecurityAnomalySignal returned."""
        det = ConfigDriftDetector()
        event = _make_config_drift_event()
        signal = det.detect(event)

        assert signal is not None
        assert signal["attack_type"] == "UNAUTHORIZED_CONFIG_CHANGE"
        assert signal["severity"] == "CRITICAL"
        assert signal["entity_id"] == "api-gateway"
        assert signal["source_ip"] == "10.0.0.5"
        assert isinstance(signal["evidence_logs"], list)
        assert len(signal["evidence_logs"]) > 0
        assert signal["confidence"] == 90.0
        assert "recommended_action" in signal
        assert signal["timestamp"] == "2026-08-02T12:00:00Z"

    def test_config_drift_in_attack_type(self) -> None:
        """CONFIG_DRIFT in attack_type field → detected."""
        det = ConfigDriftDetector()
        event = {"attack_type": "CONFIG_DRIFT", "entity_id": "order-service"}
        signal = det.detect(event)
        assert signal is not None
        assert signal["attack_type"] == "UNAUTHORIZED_CONFIG_CHANGE"

    def test_config_drift_in_attributes(self) -> None:
        """config_file_changed in attributes → detected."""
        det = ConfigDriftDetector()
        event = {
            "entity_id": "db-host",
            "attributes": {"event_type": "config_file_changed"},
        }
        signal = det.detect(event)
        assert signal is not None
        assert signal["entity_id"] == "db-host"

    def test_normal_event_not_detected(self) -> None:
        """Non-config-drift event → None returned."""
        det = ConfigDriftDetector()
        signal = det.detect(_make_normal_event())
        assert signal is None

    def test_empty_event_not_detected(self) -> None:
        """Empty event dict → None."""
        det = ConfigDriftDetector()
        assert det.detect({}) is None

    def test_non_dict_event_not_detected(self) -> None:
        """Non-dict input → None (defensive)."""
        det = ConfigDriftDetector()
        assert det.detect("not a dict") is None  # type: ignore[arg-type]
        assert det.detect(None) is None  # type: ignore[arg-type]
        assert det.detect(42) is None  # type: ignore[arg-type]

    def test_approved_change_not_flagged(self) -> None:
        """Approved change_id → None (suppressed)."""
        det = ConfigDriftDetector(approved_changes={"CHG-001"})
        event = _make_config_drift_event(change_id="CHG-001")
        signal = det.detect(event)
        assert signal is None

    def test_approved_file_path_not_flagged(self) -> None:
        """Approved file path → None."""
        det = ConfigDriftDetector(approved_changes={"/etc/nginx/nginx.conf"})
        event = _make_config_drift_event(file="/etc/nginx/nginx.conf")
        signal = det.detect(event)
        assert signal is None

    def test_unapproved_change_flagged(self) -> None:
        """Change not in approved list → flagged."""
        det = ConfigDriftDetector(approved_changes={"CHG-001"})
        event = _make_config_drift_event(change_id="CHG-999")
        signal = det.detect(event)
        assert signal is not None
        assert signal["attack_type"] == "UNAUTHORIZED_CONFIG_CHANGE"

    def test_missing_entity_id_defaults_to_unknown(self) -> None:
        """Event without entity_id → entity_id='unknown'."""
        det = ConfigDriftDetector()
        event = {"description": "config_file_changed /etc/hosts"}
        signal = det.detect(event)
        assert signal is not None
        assert signal["entity_id"] == "unknown"

    def test_missing_source_ip(self) -> None:
        """Event without source_ip → source_ip=None."""
        det = ConfigDriftDetector()
        event = {"entity_id": "web-01", "description": "config_file_changed"}
        signal = det.detect(event)
        assert signal is not None
        assert signal["source_ip"] is None

    def test_missing_timestamp_generates_default(self) -> None:
        """Event without timestamp → auto-generated ISO timestamp."""
        det = ConfigDriftDetector()
        event = {"entity_id": "web-01", "description": "CONFIG_DRIFT"}
        signal = det.detect(event)
        assert signal is not None
        assert "T" in signal["timestamp"]  # ISO 8601 format

    def test_evidence_logs_populated(self) -> None:
        """Evidence logs should contain relevant event fields."""
        det = ConfigDriftDetector()
        event = _make_config_drift_event(
            description="config_file_changed postgresql.conf",
            extra_info="some detail",
        )
        signal = det.detect(event)
        assert signal is not None
        evidence = signal["evidence_logs"]
        assert any("config_file_changed" in e for e in evidence)

    def test_evidence_raw_event_fallback(self) -> None:
        """When no recognized fields, raw_event is included."""
        det = ConfigDriftDetector()
        event = {"entity_id": "x", "event": "CONFIG_DRIFT raw payload"}
        signal = det.detect(event)
        assert signal is not None
        assert any("raw_event" in e for e in signal["evidence_logs"])

    def test_severity_always_critical(self) -> None:
        """UNAUTHORIZED_CONFIG_CHANGE always has CRITICAL severity."""
        det = ConfigDriftDetector()
        event = _make_config_drift_event()
        signal = det.detect(event)
        assert signal is not None
        assert signal["severity"] == "CRITICAL"

    def test_multiple_approved_changes(self) -> None:
        """Multiple approved IDs — only matching one is suppressed."""
        det = ConfigDriftDetector(approved_changes={"CHG-001", "CHG-002"})
        # Approved
        assert det.detect(_make_config_drift_event(change_id="CHG-001")) is None
        assert det.detect(_make_config_drift_event(change_id="CHG-002")) is None
        # Not approved
        assert det.detect(_make_config_drift_event(change_id="CHG-003")) is not None


# ─── Tests: ConfigDriftDetector mutators ─────────────────────────────────── #


class TestConfigDriftDetectorMutators:
    """Test approved-change list management."""

    def test_add_approved(self) -> None:
        det = ConfigDriftDetector()
        det.add_approved("CHG-010")
        assert "CHG-010" in det.approved_changes

    def test_remove_approved(self) -> None:
        det = ConfigDriftDetector(approved_changes={"CHG-010"})
        det.remove_approved("CHG-010")
        assert "CHG-010" not in det.approved_changes

    def test_remove_nonexistent_no_error(self) -> None:
        det = ConfigDriftDetector()
        det.remove_approved("NONEXISTENT")  # no-op

    def test_approved_changes_returns_frozenset(self) -> None:
        det = ConfigDriftDetector(approved_changes={"A", "B"})
        result = det.approved_changes
        assert isinstance(result, frozenset)
        assert result == frozenset({"A", "B"})

    def test_rule_property(self) -> None:
        det = ConfigDriftDetector()
        rule = det.rule
        assert isinstance(rule, dict)
        assert "trigger" in rule or "severity" in rule
