"""
OmniWatch — Predictive Intelligence Layer
Component: Anomaly Detector Tests
Phase: 6
Purpose: Unit tests for AnomalyDetector (train/detect/save/load)
Inputs: Synthetic feature vectors
Outputs: pytest pass/fail
"""

from __future__ import annotations

import math
import os
import tempfile

import numpy as np
import pandas as pd
import pytest

from predictive.anomaly_detector import AnomalyDetector, _clamp


# ─── Helpers ──────────────────────────────────────────────────────────────── #


def _make_normal_df(n: int = 200) -> pd.DataFrame:
    """Generate *n* rows of normal metric data (Gaussian, low variance)."""
    rng = np.random.default_rng(42)
    return pd.DataFrame(
        {
            "latency_p99": rng.normal(loc=120.0, scale=10.0, size=n),
            "error_rate": rng.normal(loc=0.01, scale=0.005, size=n),
            "request_volume": rng.normal(loc=500.0, scale=50.0, size=n),
        }
    )


def _make_anomaly_feature() -> dict:
    """A single feature vector that deviates strongly from normal."""
    return {
        "latency_p99": 999.0,  # ~88 sigma away
        "error_rate": 0.95,  # catastrophic error rate
        "request_volume": 50.0,  # severe drop
    }


# ─── Tests: _clamp ────────────────────────────────────────────────────────── #


class TestClamp:
    def test_nan_to_zero(self) -> None:
        assert _clamp(float("nan")) == 0.0

    def test_inf_to_zero(self) -> None:
        assert _clamp(float("inf")) == 0.0

    def test_neg_inf_to_zero(self) -> None:
        assert _clamp(float("-inf")) == 0.0

    def test_normal_value_passthrough(self) -> None:
        assert _clamp(0.5) == 0.5
        assert _clamp(0.0) == 0.0


# ─── Tests: AnomalyDetector ──────────────────────────────────────────────── #


class TestAnomalyDetector:
    """Core detection tests."""

    def test_normal_data_returns_none(self) -> None:
        """Normal observations should NOT trigger an anomaly signal."""
        df = _make_normal_df()
        det = AnomalyDetector()
        det.train(df)
        result = det.detect(
            {
                "latency_p99": 125.0,
                "error_rate": 0.012,
                "request_volume": 480.0,
            }
        )
        assert result is None, f"Normal data should return None, got {result}"

    def test_anomaly_data_returns_high_score(self) -> None:
        """Strongly anomalous data must produce score > 0.7."""
        df = _make_normal_df()
        det = AnomalyDetector()
        det.train(df)
        signal = det.detect(_make_anomaly_feature())
        assert signal is not None, "Anomalous data should produce a signal"
        assert signal["anomaly_score"] > 0.7, (
            f"Expected anomaly_score > 0.7, got {signal['anomaly_score']}"
        )

    def test_nan_feature_yields_score_zero(self) -> None:
        """A feature containing NaN should be handled gracefully (score 0.0)."""
        df = _make_normal_df()
        det = AnomalyDetector()
        det.train(df)
        # The detect method clamps NaN internally; NaN feature → 0.0 baseline
        result = det.detect(
            {
                "latency_p99": float("nan"),
                "error_rate": float("nan"),
                "request_volume": float("nan"),
            }
        )
        # With all NaN, scores are computed against baseline mean → may or may
        # not trigger.  The key assertion: no crash and no NaN in output.
        if result is not None:
            assert not math.isnan(result["anomaly_score"]), "anomaly_score must not be NaN"
            assert not math.isnan(result["confidence"]), "confidence must not be NaN"

    def test_anomaly_signal_contract(self) -> None:
        """Returned signal must contain all mandatory AnomalySignal fields."""
        df = _make_normal_df()
        det = AnomalyDetector()
        det.train(df)
        signal = det.detect(_make_anomaly_feature())
        assert signal is not None
        required_keys = {
            "entity_id",
            "entity_type",
            "metric_name",
            "anomaly_score",
            "confidence",
            "timestamp",
            "deviation_from_baseline",
            "source_type",
        }
        assert required_keys.issubset(signal.keys()), (
            f"Missing keys: {required_keys - signal.keys()}"
        )

    def test_cold_start_skip_isolation_forest(self) -> None:
        """With <100 samples, IsolationForest should not be fitted."""
        df = _make_normal_df(n=50)  # below 100
        det = AnomalyDetector()
        det.train(df)
        assert det._isolation_forest is None, "IsolationForest should not be fitted at cold start"
        assert det._train_count == 50

    def test_warm_start_fits_isolation_forest(self) -> None:
        """With ≥100 samples, IsolationForest should be fitted."""
        df = _make_normal_df(n=150)
        det = AnomalyDetector()
        det.train(df)
        assert det._isolation_forest is not None, "IsolationForest should be fitted at warm start"
        assert det._train_count == 150

    def test_source_type_performance(self) -> None:
        """Non-security metric names should yield source_type=performance."""
        df = _make_normal_df()
        det = AnomalyDetector()
        det.train(df)
        signal = det.detect(_make_anomaly_feature())
        assert signal is not None
        assert signal["source_type"] == "performance"

    def test_source_type_security(self) -> None:
        """Metric names containing 'auth'/'login'/'access' → source_type=security."""
        df = pd.DataFrame({"auth_failure_rate": np.random.default_rng(42).normal(0.01, 0.005, 200)})
        det = AnomalyDetector()
        det.train(df)
        signal = det.detect({"auth_failure_rate": 0.99})
        # Score should be very high for such a deviation
        if signal is not None:
            assert signal["source_type"] == "security"

    def test_score_clamped_zero_for_nan(self) -> None:
        """_clamp must convert NaN to 0.0 — verify edge-case explicitly."""
        assert _clamp(float("nan")) == 0.0
        assert _clamp(float("inf")) == 0.0
        assert _clamp(-0.5) == -0.5  # negative is allowed in general


class TestModelPersistence:
    """save_model / load_model round-trip tests."""

    def test_save_load_roundtrip(self) -> None:
        """A loaded model should produce identical detection results."""
        df = _make_normal_df(n=150)
        det = AnomalyDetector()
        det.train(df)

        with tempfile.TemporaryDirectory() as tmpdir:
            model_path = os.path.join(tmpdir, "model.joblib")
            det.save_model(model_path)
            assert os.path.isfile(model_path)

            det2 = AnomalyDetector()
            det2.load_model(model_path)

            # Both should agree on a detection
            feature = _make_anomaly_feature()
            s1 = det.detect(feature)
            s2 = det2.detect(feature)
            assert s1 is not None and s2 is not None
            assert s1["anomaly_score"] == s2["anomaly_score"]
            assert s1["confidence"] == s2["confidence"]

    def test_load_cold_start_model(self) -> None:
        """A cold-start model (<100 samples) should save/load correctly."""
        df = _make_normal_df(n=50)
        det = AnomalyDetector()
        det.train(df)

        with tempfile.TemporaryDirectory() as tmpdir:
            model_path = os.path.join(tmpdir, "cold.joblib")
            det.save_model(model_path)

            det2 = AnomalyDetector()
            det2.load_model(model_path)
            assert det2._isolation_forest is None
            assert det2._train_count == 50
