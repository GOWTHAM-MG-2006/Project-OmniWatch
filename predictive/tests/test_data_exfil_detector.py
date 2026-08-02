"""Tests for the DataExfilDetector — outbound traffic spike detection."""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict

import pytest

from predictive.security.data_exfil_detector import (
    DEFAULT_CONFIDENCE,
    DEFAULT_OUTBOUND_RATIO,
    DEFAULT_SEVERITY,
    DataExfilDetector,
    _load_rules,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(epoch: int) -> str:
    """Return an ISO-8601 timestamp string from an epoch integer."""
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


def _event(
    entity_id: str = "svc-web-01",
    outbound_bytes: float = 1000.0,
    epoch: int = 1_700_000_000,
    source_ip: str | None = "10.0.0.5",
) -> Dict[str, Any]:
    """Build a minimal event dict for DataExfilDetector.detect()."""
    evt: Dict[str, Any] = {
        "entity_id": entity_id,
        "outbound_bytes": outbound_bytes,
        "timestamp": _ts(epoch),
    }
    if source_ip is not None:
        evt["source_ip"] = source_ip
    return evt


# ---------------------------------------------------------------------------
# _load_rules
# ---------------------------------------------------------------------------

class TestLoadRules:
    """Unit tests for the YAML rule loader."""

    def test_loads_from_default_path(self) -> None:
        rules = _load_rules()
        assert "outbound_ratio" in rules

    def test_missing_file_returns_empty(self, tmp_path: Path) -> None:
        rules = _load_rules(str(tmp_path / "nonexistent.yaml"))
        assert rules == {}

    def test_corrupt_yaml_returns_empty(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.yaml"
        bad.write_text("{{invalid yaml::", encoding="utf-8")
        rules = _load_rules(str(bad))
        assert rules == {}


# ---------------------------------------------------------------------------
# Detection — basic behaviour
# ---------------------------------------------------------------------------

class TestDetectionBasic:
    """Core detection logic tests."""

    def test_first_event_returns_none(self) -> None:
        """First observation has no baseline → no detection."""
        det = DataExfilDetector(outbound_ratio=3.0)
        result = det.detect(_event(outbound_bytes=5000))
        assert result is None

    def test_normal_traffic_not_detected(self) -> None:
        """Traffic within the threshold ratio → no detection."""
        det = DataExfilDetector(outbound_ratio=3.0)
        # Seed with baseline values
        for i in range(5):
            det.detect(_event(outbound_bytes=1000, epoch=1_700_000_000 + i * 10))
        # Send traffic at 1× average → no detection
        result = det.detect(_event(outbound_bytes=1000, epoch=1_700_000_060))
        assert result is None

    def test_5x_spike_detected(self) -> None:
        """5× outbound spike → DATA_EXFILTRATION detected."""
        det = DataExfilDetector(outbound_ratio=3.0)
        # Seed baseline: 5 observations at 1000 bytes
        for i in range(5):
            det.detect(_event(outbound_bytes=1000, epoch=1_700_000_000 + i * 10))
        # Spike: 5000 bytes (5× average of 1000)
        result = det.detect(_event(outbound_bytes=5000, epoch=1_700_000_060))
        assert result is not None
        assert result["attack_type"] == "DATA_EXFILTRATION"

    def test_exactly_at_threshold_not_detected(self) -> None:
        """Traffic exactly at the ratio threshold → no detection (uses <=)."""
        det = DataExfilDetector(outbound_ratio=3.0)
        for i in range(5):
            det.detect(_event(outbound_bytes=1000, epoch=1_700_000_000 + i * 10))
        # 3× average → exactly at threshold → not detected
        result = det.detect(_event(outbound_bytes=3000, epoch=1_700_000_060))
        assert result is None

    def test_above_threshold_detected(self) -> None:
        """Traffic above threshold → detected."""
        det = DataExfilDetector(outbound_ratio=3.0)
        for i in range(5):
            det.detect(_event(outbound_bytes=1000, epoch=1_700_000_000 + i * 10))
        # 3100 bytes → 3.1× → above threshold
        result = det.detect(_event(outbound_bytes=3100, epoch=1_700_000_060))
        assert result is not None


# ---------------------------------------------------------------------------
# SecurityAnomalySignal contract
# ---------------------------------------------------------------------------

class TestSignalContract:
    """Verify every required field in the returned signal."""

    def _detect_spike(self) -> Dict[str, Any]:
        det = DataExfilDetector(outbound_ratio=3.0)
        for i in range(5):
            det.detect(_event(outbound_bytes=1000, epoch=1_700_000_000 + i * 10))
        result = det.detect(_event(outbound_bytes=5000, epoch=1_700_000_060))
        assert result is not None
        return result

    def test_attack_type(self) -> None:
        sig = self._detect_spike()
        assert sig["attack_type"] == "DATA_EXFILTRATION"

    def test_entity_id(self) -> None:
        sig = self._detect_spike()
        assert sig["entity_id"] == "svc-web-01"

    def test_severity(self) -> None:
        sig = self._detect_spike()
        assert sig["severity"] == DEFAULT_SEVERITY

    def test_confidence(self) -> None:
        sig = self._detect_spike()
        assert sig["confidence"] == DEFAULT_CONFIDENCE

    def test_evidence_logs_is_list(self) -> None:
        sig = self._detect_spike()
        assert isinstance(sig["evidence_logs"], list)
        assert len(sig["evidence_logs"]) >= 3

    def test_recommended_action_contains_entity(self) -> None:
        sig = self._detect_spike()
        assert "svc-web-01" in sig["recommended_action"]

    def test_source_ip_propagated(self) -> None:
        sig = self._detect_spike()
        assert sig["source_ip"] == "10.0.0.5"

    def test_timestamp_is_iso(self) -> None:
        sig = self._detect_spike()
        # Should parse without error
        dt = datetime.fromisoformat(sig["timestamp"])
        assert dt.tzinfo is not None


# ---------------------------------------------------------------------------
# Rolling window & eviction
# ---------------------------------------------------------------------------

class TestRollingWindow:
    """Verify the rolling window mechanics."""

    def test_stale_entries_evicted(self) -> None:
        """Entries outside the window are dropped from the average."""
        det = DataExfilDetector(outbound_ratio=3.0, window_seconds=120)
        # Seed at t=100, 110, 120
        det.detect(_event(outbound_bytes=1000, epoch=100))
        det.detect(_event(outbound_bytes=1000, epoch=110))
        det.detect(_event(outbound_bytes=1000, epoch=120))
        # At t=250: cutoff=250-120=130 → entries at 100, 110, 120 all evicted
        # No baseline → None
        result = det.detect(_event(outbound_bytes=9999, epoch=250))
        assert result is None
        # At t=175: cutoff=175-120=55 → entries at 100,110,120 all within window
        # avg=1000, spike=3500 → 3.5× → detected
        det2 = DataExfilDetector(outbound_ratio=3.0, window_seconds=120)
        det2.detect(_event(outbound_bytes=1000, epoch=100))
        det2.detect(_event(outbound_bytes=1000, epoch=110))
        det2.detect(_event(outbound_bytes=1000, epoch=120))
        result2 = det2.detect(_event(outbound_bytes=3500, epoch=175))
        assert result2 is not None

    def test_window_respected(self) -> None:
        """All entries outside window are evicted, average resets."""
        det = DataExfilDetector(outbound_ratio=3.0, window_seconds=30)
        # Seed at t=100
        det.detect(_event(outbound_bytes=1000, epoch=100))
        # At t=200 (100s later), all entries evicted → no baseline
        result = det.detect(_event(outbound_bytes=9999, epoch=200))
        assert result is None

    def test_rolling_average_updates(self) -> None:
        """New observations shift the rolling average."""
        det = DataExfilDetector(outbound_ratio=3.0, window_seconds=300)
        # Seed: avg of [500, 500, 500] = 500
        for i in range(3):
            det.detect(_event(outbound_bytes=500, epoch=100 + i * 10))
        # Add 2000 → avg of [500, 500, 500, 2000] = 875
        det.detect(_event(outbound_bytes=2000, epoch=140))
        # Now avg ≈ 875.  Spike at 3000 → 3000/875 ≈ 3.43× → detected
        result = det.detect(_event(outbound_bytes=3000, epoch=160))
        assert result is not None


# ---------------------------------------------------------------------------
# Edge cases & error handling
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """Edge cases: missing fields, bad data, etc."""

    def test_missing_entity_id(self) -> None:
        det = DataExfilDetector(outbound_ratio=3.0)
        result = det.detect({"outbound_bytes": 1000, "timestamp": _ts(100)})
        assert result is None

    def test_missing_outbound_bytes(self) -> None:
        det = DataExfilDetector(outbound_ratio=3.0)
        result = det.detect({"entity_id": "svc", "timestamp": _ts(100)})
        assert result is None

    def test_missing_timestamp(self) -> None:
        det = DataExfilDetector(outbound_ratio=3.0)
        result = det.detect({"entity_id": "svc", "outbound_bytes": 1000})
        assert result is None

    def test_non_numeric_outbound_bytes(self) -> None:
        det = DataExfilDetector(outbound_ratio=3.0)
        result = det.detect({
            "entity_id": "svc",
            "outbound_bytes": "not_a_number",
            "timestamp": _ts(100),
        })
        assert result is None

    def test_invalid_timestamp(self) -> None:
        det = DataExfilDetector(outbound_ratio=3.0)
        result = det.detect({
            "entity_id": "svc",
            "outbound_bytes": 1000,
            "timestamp": "not-a-timestamp",
        })
        assert result is None

    def test_none_source_ip(self) -> None:
        """Missing source_ip → signal has source_ip=None."""
        det = DataExfilDetector(outbound_ratio=3.0)
        for i in range(5):
            det.detect(_event(outbound_bytes=1000, epoch=1_700_000_000 + i * 10))
        result = det.detect(_event(
            outbound_bytes=5000,
            epoch=1_700_000_060,
            source_ip=None,
        ))
        assert result is not None
        assert result["source_ip"] is None

    def test_empty_event(self) -> None:
        det = DataExfilDetector(outbound_ratio=3.0)
        result = det.detect({})
        assert result is None


# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------

class TestReset:
    """Verify reset() clears all state."""

    def test_reset_clears_baseline(self) -> None:
        det = DataExfilDetector(outbound_ratio=3.0)
        for i in range(5):
            det.detect(_event(outbound_bytes=1000, epoch=1_700_000_000 + i * 10))
        det.reset()
        # After reset, no baseline → returns None
        result = det.detect(_event(outbound_bytes=9999, epoch=1_700_000_060))
        assert result is None


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class TestConfiguration:
    """Verify config loading and overrides."""

    def test_custom_ratio(self) -> None:
        """Custom outbound_ratio overrides the default."""
        # 4× spike → below 5× threshold → not detected
        det = DataExfilDetector(outbound_ratio=5.0)
        for i in range(5):
            det.detect(_event(outbound_bytes=1000, epoch=1_700_000_000 + i * 10))
        result = det.detect(_event(outbound_bytes=4000, epoch=1_700_000_060))
        assert result is None
        # 6× spike → above 5× threshold → detected (fresh detector)
        det2 = DataExfilDetector(outbound_ratio=5.0)
        for i in range(5):
            det2.detect(_event(outbound_bytes=1000, epoch=1_700_000_000 + i * 10))
        result2 = det2.detect(_event(outbound_bytes=6000, epoch=1_700_000_060))
        assert result2 is not None

    def test_default_ratio_from_yaml(self) -> None:
        """Default ratio loaded from security_rules.yaml = 3.0."""
        det = DataExfilDetector()
        assert det._outbound_ratio == DEFAULT_OUTBOUND_RATIO

    def test_severity_from_defaults(self) -> None:
        """Default severity matches DEFAULT_SEVERITY."""
        det = DataExfilDetector()
        assert det._severity == DEFAULT_SEVERITY


# ---------------------------------------------------------------------------
# Multi-entity isolation
# ---------------------------------------------------------------------------

class TestMultiEntity:
    """Verify per-entity isolation of rolling windows."""

    def test_independent_entities(self) -> None:
        """Each entity maintains its own rolling window."""
        det = DataExfilDetector(outbound_ratio=3.0)
        # Seed entity-a with 1000
        for i in range(5):
            det.detect(_event(
                entity_id="entity-a",
                outbound_bytes=1000,
                epoch=1_700_000_000 + i * 10,
            ))
        # Seed entity-b with 5000
        for i in range(5):
            det.detect(_event(
                entity_id="entity-b",
                outbound_bytes=5000,
                epoch=1_700_000_000 + i * 10,
            ))
        # entity-a: 3100 → 3.1× avg of 1000 → detected
        result_a = det.detect(_event(
            entity_id="entity-a",
            outbound_bytes=3100,
            epoch=1_700_000_060,
        ))
        assert result_a is not None

        # entity-b: 5100 → 1.02× avg of 5000 → not detected
        result_b = det.detect(_event(
            entity_id="entity-b",
            outbound_bytes=5100,
            epoch=1_700_000_060,
        ))
        assert result_b is None


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------

class TestThreadSafety:
    """Concurrent detect() calls should not corrupt state."""

    def test_concurrent_access(self) -> None:
        det = DataExfilDetector(outbound_ratio=3.0)
        errors: list[Exception] = []

        def worker(entity: str, start_epoch: int) -> None:
            try:
                for i in range(10):
                    det.detect(_event(
                        entity_id=entity,
                        outbound_bytes=1000,
                        epoch=start_epoch + i * 10,
                    ))
            except Exception as exc:
                errors.append(exc)

        threads = [
            threading.Thread(target=worker, args=(f"entity-{j}", 1_700_000_000 + j * 1000))
            for j in range(10)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert errors == [], f"Thread errors: {errors}"
