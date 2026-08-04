"""
OmniWatch — Predictive Intelligence Layer
Component: Flag-combination tests (Task T11)
Phase: 6
Purpose: Verify the full DetectorEngine.process_message() pipeline produces
         valid output for all 8 permutations of the three detection flags
         (if_enabled x zscore_enabled x security_enabled).
Inputs: None
Outputs: pytest pass/fail
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from types import ModuleType
from typing import Any, Dict, Optional

import pytest

# ---------------------------------------------------------------------------
# Pre-inject a fake ``kafka`` module so any lazy kafka-python-ng import
# resolves without importing the real package (broken on Python 3.14).
# ---------------------------------------------------------------------------
_fake_kafka = ModuleType("kafka")
_fake_kafka.KafkaProducer = __import__("unittest.mock").mock.MagicMock  # type: ignore[attr-defined]
_fake_kafka.KafkaConsumer = __import__("unittest.mock").mock.MagicMock  # type: ignore[attr-defined]
sys.modules.setdefault("kafka", _fake_kafka)

from predictive.config.settings import Settings
from predictive.detector_engine import DetectorEngine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_settings(**overrides: Any) -> Settings:
    """Build a Settings isolated from dotenv leakage."""
    return Settings(_env_file=None, **overrides)


def _sample_feature(**extra: Any) -> Dict[str, Any]:
    """A minimal feature vector dict (as returned by FeatureReader)."""
    base: Dict[str, Any] = {
        "entity_id": "order-service",
        "latency_p99": 150.0,
        "error_rate": 0.02,
        "request_volume": 1000.0,
    }
    base.update(extra)
    return base


def _sample_signal(**overrides: Any) -> Dict[str, Any]:
    """A valid AnomalySignal dict."""
    base: Dict[str, Any] = {
        "entity_id": "order-service",
        "entity_type": "API_NODE",
        "metric_name": "latency_p99",
        "anomaly_score": 0.85,
        "confidence": 78.0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "deviation_from_baseline": 2.3,
        "source_type": "performance",
    }
    base.update(overrides)
    return base


class _FakeDetector:
    """Minimal AnomalyDetector stand-in for pipeline tests."""

    def __init__(self, return_signal: Optional[Dict[str, Any]] = None) -> None:
        self._return_signal = return_signal
        self.detect_calls: list = []
        self.train_calls: list = []

    def detect(self, feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        self.detect_calls.append(feature)
        return self._return_signal

    def train(self, df: Any) -> None:
        self.train_calls.append(df)


class _FakeThresholder:
    """Minimal AdaptiveThresholder stand-in (no adaptive gate)."""

    def get_threshold(self, entity_id: str, metric: str) -> Optional[float]:
        return None

    def update(self, entity_id: str, metric: str, value: float) -> None:
        pass


class _FakeNoiseFilter:
    """Minimal NoiseFilter stand-in (never suppresses)."""

    def should_suppress(self, **kwargs: Any) -> bool:
        return False


class _FakeEnricher:
    """Minimal SignalEnricher stand-in."""

    def enrich(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        enriched = dict(signal)
        enriched["entity_context"] = {"name": "order-service", "criticality": "high"}
        enriched["enriched"] = True
        return enriched


class _FakeProducer:
    """Minimal AnomalyProducer stand-in."""

    def __init__(self) -> None:
        self.publish_calls: list = []

    def publish(self, signal: Dict[str, Any]) -> None:
        self.publish_calls.append(signal)

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Flag-combination tests (Task T11)
# ---------------------------------------------------------------------------
#
# The plan's "8 permutations of if_enabled x zscore_enabled x security_enabled"
# refers to three detection flags.  Only ``predictive_security_enabled`` exists
# as a Settings field today; ``if_enabled`` and ``zscore_enabled`` are NOT
# configurable knobs on the engine or detector (they are always-on paths in
# AnomalyDetector).  Per the T11 decision, we do NOT invent new Settings fields
# or modify the engine.  Instead:
#
#   * ``security_enabled``  -> toggles ``predictive_security_enabled`` (real flag)
#   * ``if_enabled``        -> controls whether the ``isolation_forest``
#                              detector contribution is present in the signal
#   * ``zscore_enabled``    -> controls whether the ``z_score`` contribution
#                              is present in the signal
#
# ``seasonal_naive`` is always present (not gated by a flag).  This exercises
# the fusion engine's graceful handling of missing detectors (missing -> 0.0)
# across every combination, through the full process_message() pipeline.


_FLAG_PERMUTATIONS = [
    (True, True, True),
    (True, True, False),
    (True, False, True),
    (False, True, True),
    (True, False, False),
    (False, True, False),
    (False, False, True),
    (False, False, False),
]


@pytest.mark.parametrize(
    "if_enabled,zscore_enabled,security_enabled", _FLAG_PERMUTATIONS
)
def test_all_flag_combinations(
    if_enabled: bool, zscore_enabled: bool, security_enabled: bool
) -> None:
    """Every flag combination produces valid output through process_message()."""
    # Build the detector contributions that the enabled detectors would emit.
    contributions: Dict[str, float] = {"seasonal_naive": 0.8}
    if zscore_enabled:
        contributions["z_score"] = 0.9
    if if_enabled:
        contributions["isolation_forest"] = 0.7

    signal = _sample_signal(
        anomaly_score=0.85,
        detector_contributions=contributions,
    )
    detector = _FakeDetector(return_signal=signal)
    producer = _FakeProducer()

    engine = DetectorEngine(
        settings=_make_settings(predictive_security_enabled=security_enabled),
        detector=detector,
        thresholder=_FakeThresholder(),
        noise_filter=_FakeNoiseFilter(),
        enricher=_FakeEnricher(),
        producer=producer,
    )
    engine._is_trained = True

    result = engine.process_message(_sample_feature())

    # A valid anomaly signal is produced and published for every combination.
    assert result is not None
    assert 0.0 <= result["anomaly_score"] <= 1.0
    assert 0.0 <= result["confidence"] <= 100.0
    assert result["entity_id"] == "order-service"
    assert result["enriched"] is True
    assert len(producer.publish_calls) == 1
    assert producer.publish_calls[0] is result


@pytest.mark.parametrize(
    "if_enabled,zscore_enabled,security_enabled", _FLAG_PERMUTATIONS
)
def test_flag_combination_fusion_score_in_range(
    if_enabled: bool, zscore_enabled: bool, security_enabled: bool
) -> None:
    """The fused anomaly_score stays in [0,1] regardless of which detectors
    contribute (fusion treats missing detectors as 0.0)."""
    contributions: Dict[str, float] = {"seasonal_naive": 0.8}
    if zscore_enabled:
        contributions["z_score"] = 0.9
    if if_enabled:
        contributions["isolation_forest"] = 0.7

    signal = _sample_signal(
        anomaly_score=0.85,
        detector_contributions=contributions,
    )
    detector = _FakeDetector(return_signal=signal)
    producer = _FakeProducer()

    engine = DetectorEngine(
        settings=_make_settings(predictive_security_enabled=security_enabled),
        detector=detector,
        thresholder=_FakeThresholder(),
        noise_filter=_FakeNoiseFilter(),
        enricher=_FakeEnricher(),
        producer=producer,
    )
    engine._is_trained = True

    result = engine.process_message(_sample_feature())

    assert result is not None
    assert 0.0 <= result["anomaly_score"] <= 1.0
    # When at least one detector contributes, fusion recalibrates the score.
    if contributions:
        assert result["anomaly_score"] != 0.85
    else:
        # No contributions -> fusion skipped -> raw score preserved.
        assert result["anomaly_score"] == 0.85


def test_security_flag_is_real_settings_field() -> None:
    """The security flag maps to the real predictive_security_enabled setting."""
    assert _make_settings(predictive_security_enabled=True).predictive_security_enabled is True
    assert _make_settings(predictive_security_enabled=False).predictive_security_enabled is False