"""Tests for the PrivilegeEscalationDetector (GAP 1 security detector)."""

from unittest.mock import patch

import pytest

from predictive.security.priv_escalation_detector import (
    PrivEscalationDetector,
    _DEFAULT_CONFIDENCE,
    _DEFAULT_SEVERITY,
)


# ── Helpers ────────────────────────────────────────────────────────────── #


def _make_event(
    log_message: str = "",
    entity_id: str = "svc-orders",
    entity_type: str = "SERVICE",
    source_ip: str | None = "10.0.0.5",
    timestamp: str | None = None,
) -> dict:
    """Build a minimal security event dict."""
    return {
        "log_message": log_message,
        "entity_id": entity_id,
        "entity_type": entity_type,
        "source_ip": source_ip,
        "timestamp": timestamp,
    }


# ── Detection: positive cases ──────────────────────────────────────────── #


class TestDetectionPositive:
    """Events that SHOULD trigger a PrivilegeEscalationSignal."""

    def test_sudo_detected(self):
        """A sudo log from a non-admin entity should be detected."""
        det = PrivEscalationDetector()
        event = _make_event(log_message="user deployer executed sudo apt-get update")
        signal = det.detect(event)

        assert signal is not None
        assert signal["attack_type"] == "PRIVILEGE_ESCALATION_ATTEMPT"
        assert signal["severity"] == "CRITICAL"
        assert signal["confidence"] == 90.0
        assert signal["entity_id"] == "svc-orders"
        assert "sudo" in signal["evidence_logs"][0]

    def test_su_dash_detected(self):
        """An 'su -' log should be detected."""
        det = PrivEscalationDetector()
        event = _make_event(log_message="user jenkins ran su - postgres")
        signal = det.detect(event)

        assert signal is not None
        assert signal["attack_type"] == "PRIVILEGE_ESCALATION_ATTEMPT"

    def test_escalat_pattern_detected(self):
        """A log containing 'escalat' (partial match for escalation) should fire."""
        det = PrivEscalationDetector()
        event = _make_event(
            log_message="System detected privilege escalation attempt on node-3"
        )
        signal = det.detect(event)

        assert signal is not None
        assert signal["attack_type"] == "PRIVILEGE_ESCALATION_ATTEMPT"
        assert signal["severity"] == "CRITICAL"

    def test_role_change_detected(self):
        """A role_change log should be detected."""
        det = PrivEscalationDetector()
        event = _make_event(
            log_message="role_change: user alice promoted to admin"
        )
        signal = det.detect(event)

        assert signal is not None
        assert signal["attack_type"] == "PRIVILEGE_ESCALATION_ATTEMPT"

    def test_case_insensitive(self):
        """Detection should be case-insensitive."""
        det = PrivEscalationDetector()
        event = _make_event(log_message="SUDO command executed by deployer")
        signal = det.detect(event)

        assert signal is not None

    def test_multiple_patterns_matched(self):
        """Event with multiple pattern hits should still produce one signal."""
        det = PrivEscalationDetector()
        event = _make_event(
            log_message="sudo su - postgres: privilege escalation detected"
        )
        signal = det.detect(event)

        assert signal is not None
        assert len(signal["evidence_logs"]) == 1

    def test_source_ip_forwarded(self):
        """The source_ip from the event should appear in the signal."""
        det = PrivEscalationDetector()
        event = _make_event(
            log_message="sudo vim /etc/shadow",
            source_ip="192.168.1.100",
        )
        signal = det.detect(event)

        assert signal is not None
        assert signal["source_ip"] == "192.168.1.100"

    def test_timestamp_provided(self):
        """When timestamp is given, it is forwarded."""
        det = PrivEscalationDetector()
        event = _make_event(
            log_message="sudo reboot",
            timestamp="2026-08-02T12:00:00Z",
        )
        signal = det.detect(event)

        assert signal is not None
        assert signal["timestamp"] == "2026-08-02T12:00:00Z"

    def test_timestamp_generated_when_missing(self):
        """When timestamp is absent, a UTC ISO timestamp is generated."""
        det = PrivEscalationDetector()
        event = _make_event(log_message="sudo ls /root")
        event.pop("timestamp", None)
        signal = det.detect(event)

        assert signal is not None
        assert signal["timestamp"]  # non-empty string

    def test_recommended_action_present(self):
        """Signal must include a recommended_action string."""
        det = PrivEscalationDetector()
        event = _make_event(log_message="sudo rm -rf /")
        signal = det.detect(event)

        assert signal is not None
        assert isinstance(signal["recommended_action"], str)
        assert len(signal["recommended_action"]) > 0


# ── Detection: negative cases ──────────────────────────────────────────── #


class TestDetectionNegative:
    """Events that should NOT trigger a signal."""

    def test_clean_log_not_detected(self):
        """A normal log with no escalation patterns returns None."""
        det = PrivEscalationDetector()
        event = _make_event(
            log_message="HTTP 200 GET /api/health latency=12ms"
        )
        assert det.detect(event) is None

    def test_empty_log_message(self):
        """An empty log_message returns None."""
        det = PrivEscalationDetector()
        event = _make_event(log_message="")
        assert det.detect(event) is None

    def test_missing_log_message(self):
        """A dict with no log_message key returns None."""
        det = PrivEscalationDetector()
        event = {"entity_id": "svc-a", "entity_type": "SERVICE"}
        assert det.detect(event) is None

    def test_admin_entity_root_skipped(self):
        """The 'root' admin entity should never trigger an alert."""
        det = PrivEscalationDetector()
        event = _make_event(
            log_message="sudo apt-get upgrade",
            entity_id="root",
        )
        assert det.detect(event) is None

    def test_admin_entity_sysadmin_skipped(self):
        """sysadmin entity should be skipped."""
        det = PrivEscalationDetector()
        event = _make_event(
            log_message="sudo systemctl restart nginx",
            entity_id="sysadmin",
        )
        assert det.detect(event) is None

    def test_admin_entity_type_skipped(self):
        """An entity with 'admin' in entity_type should be skipped."""
        det = PrivEscalationDetector()
        event = _make_event(
            log_message="sudo reboot",
            entity_id="svc-ops",
            entity_type="ADMIN_SERVICE",
        )
        assert det.detect(event) is None

    def test_admin_entity_sre_skipped(self):
        """sre entity should be skipped."""
        det = PrivEscalationDetector()
        event = _make_event(
            log_message="sudo kubectl get pods",
            entity_id="sre",
        )
        assert det.detect(event) is None

    def test_word_boundary_no_false_positive(self):
        """'hustle' should not match 'sudo' due to word boundary."""
        det = PrivEscalationDetector()
        event = _make_event(
            log_message="user hustled through the deployment queue"
        )
        assert det.detect(event) is None


# ── Configuration overrides ────────────────────────────────────────────── #


class TestConfiguration:
    """Test that constructor overrides and YAML loading work."""

    def test_default_config_values(self):
        """Defaults match the module-level constants."""
        det = PrivEscalationDetector()
        # No way to directly access _severity/_confidence without privates,
        # but detection behavior confirms they are set.
        event = _make_event(log_message="sudo whoami")
        signal = det.detect(event)
        assert signal is not None
        assert signal["severity"] == _DEFAULT_SEVERITY
        assert signal["confidence"] == _DEFAULT_CONFIDENCE

    def test_custom_severity_and_confidence(self):
        """Constructor overrides for severity and confidence."""
        det = PrivEscalationDetector(severity="HIGH", confidence=75.0)
        event = _make_event(log_message="sudo ls")
        signal = det.detect(event)

        assert signal is not None
        assert signal["severity"] == "HIGH"
        assert signal["confidence"] == 75.0

    def test_custom_patterns(self):
        """Constructor overrides for detection patterns."""
        det = PrivEscalationDetector(patterns=["elevate", "promote"])
        # Should match new pattern
        event = _make_event(log_message="user bob was elevated to superuser")
        signal = det.detect(event)
        assert signal is not None

        # Should NOT match old patterns
        event2 = _make_event(log_message="sudo whoami")
        assert det.detect(event2) is None

    def test_custom_admin_identifiers(self):
        """Custom admin identifier set."""
        det = PrivEscalationDetector(admin_identifiers=frozenset({"superuser"}))
        event = _make_event(
            log_message="sudo apt-get install",
            entity_id="superuser",
        )
        assert det.detect(event) is None

    def test_yaml_loading_success(self, tmp_path, monkeypatch):
        """When YAML is available and valid, patterns/severity are loaded."""
        yaml_content = (
            "attack_types:\n"
            "  priv_escalation:\n"
            "    patterns: [\"escalate\"]\n"
            "    severity: HIGH\n"
        )
        yaml_file = tmp_path / "security_rules.yaml"
        yaml_file.write_text(yaml_content, encoding="utf-8")

        monkeypatch.setattr(
            "predictive.security.priv_escalation_detector._RULES_PATH", yaml_file
        )

        # Force re-import of yaml
        import importlib
        import predictive.security.priv_escalation_detector as mod

        with patch.dict("sys.modules", {"yaml": __import__("yaml")}):
            det = PrivEscalationDetector()
            event = _make_event(log_message="escalate privileges")
            signal = det.detect(event)

            assert signal is not None
            assert signal["severity"] == "HIGH"

    def test_yaml_file_missing_uses_defaults(self, monkeypatch):
        """When YAML file is missing, defaults are used."""
        monkeypatch.setattr(
            "predictive.security.priv_escalation_detector._RULES_PATH",
            "/nonexistent/path.yaml",
        )
        det = PrivEscalationDetector()
        event = _make_event(log_message="sudo whoami")
        signal = det.detect(event)

        assert signal is not None
        assert signal["severity"] == _DEFAULT_SEVERITY


# ── SecurityAnomalySignal contract ────────────────────────────────────── #


class TestSignalContract:
    """Verify the output dict conforms to the SecurityAnomalySignal contract."""

    REQUIRED_KEYS = {
        "attack_type",
        "entity_id",
        "severity",
        "confidence",
        "evidence_logs",
        "recommended_action",
        "source_ip",
        "timestamp",
    }

    def test_all_required_keys_present(self):
        """Signal must contain all SecurityAnomalySignal keys."""
        det = PrivEscalationDetector()
        event = _make_event(log_message="sudo shutdown -h now")
        signal = det.detect(event)

        assert signal is not None
        assert self.REQUIRED_KEYS.issubset(signal.keys()), (
            f"Missing keys: {self.REQUIRED_KEYS - signal.keys()}"
        )

    def test_evidence_logs_is_list(self):
        """evidence_logs must be a list of strings."""
        det = PrivEscalationDetector()
        event = _make_event(log_message="sudo reboot")
        signal = det.detect(event)

        assert signal is not None
        assert isinstance(signal["evidence_logs"], list)
        assert all(isinstance(s, str) for s in signal["evidence_logs"])

    def test_source_ip_type(self):
        """source_ip is str or None."""
        det = PrivEscalationDetector()
        event = _make_event(log_message="sudo ls", source_ip=None)
        signal = det.detect(event)

        assert signal is not None
        assert signal["source_ip"] is None

    def test_confidence_is_numeric(self):
        """confidence should be a number (int or float)."""
        det = PrivEscalationDetector()
        event = _make_event(log_message="sudo cat /etc/passwd")
        signal = det.detect(event)

        assert signal is not None
        assert isinstance(signal["confidence"], (int, float))
        assert 0 <= signal["confidence"] <= 100


# ── Edge cases ──────────────────────────────────────────────────────────── #


class TestEdgeCases:
    """Edge cases and boundary conditions."""

    def test_empty_entity_id(self):
        """An empty entity_id is not treated as admin."""
        det = PrivEscalationDetector()
        event = _make_event(log_message="sudo whoami", entity_id="")
        signal = det.detect(event)

        assert signal is not None

    def test_non_string_log_message(self):
        """Non-string log_message is coerced to str."""
        det = PrivEscalationDetector()
        event = _make_event(log_message=12345)  # type: ignore[arg-type]
        signal = det.detect(event)

        # 12345 as string won't match any pattern
        assert signal is None

    def test_extra_event_fields_ignored(self):
        """Extra fields in the event dict do not cause errors."""
        det = PrivEscalationDetector()
        event = _make_event(log_message="sudo vim /etc/passwd")
        event["extra_key"] = "extra_value"
        event["another"] = 42
        signal = det.detect(event)

        assert signal is not None

    def test_neutral_entity_detected(self):
        """A neutral (non-admin) entity with escalation should trigger."""
        det = PrivEscalationDetector()
        event = _make_event(
            log_message="user dev executed sudo make install",
            entity_id="dev-workstation",
            entity_type="WORKSTATION",
        )
        signal = det.detect(event)

        assert signal is not None
        assert signal["entity_id"] == "dev-workstation"

    def test_pattern_in_middle_of_line(self):
        """Pattern match should work anywhere in the log line."""
        det = PrivEscalationDetector()
        event = _make_event(
            log_message="[2026-08-02T12:00:00Z] user=deployer action=sudo target=/bin/bash"
        )
        signal = det.detect(event)

        assert signal is not None

    def test_sudo_in_quoted_string(self):
        """Sudo in a quoted argument should still match."""
        det = PrivEscalationDetector()
        event = _make_event(
            log_message="script executed: 'sudo chmod 777 /tmp'"
        )
        signal = det.detect(event)

        assert signal is not None
