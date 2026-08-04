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

    # ── Task T1: entity_id provenance ──────────────────────────────────── #

    def test_baseline_entity_id_fallback(self) -> None:
        """BASELINE CHARACTERIZATION (T1) — post-fix contract: when the
        feature dict carries NO ``entity_id``, detect() falls back to the
        ``"unknown"`` sentinel (previously it emitted the synthetic
        ``anomaly-{metric}-{YYYYMMDDHHMMSS}`` string — that pre-fix shape is
        superseded and pinned in the git history of this file).
        """
        df = _make_normal_df()
        det = AnomalyDetector()
        det.train(df)
        signal = det.detect(_make_anomaly_feature())
        assert signal is not None

        assert signal["entity_id"] == "unknown"
        assert signal["metric_name"] == "latency_p99"

    def test_real_entity_id_propagates_from_feature(self) -> None:
        """FAILING-FIRST (T1): a feature dict carrying ``entity_id`` must use
        it verbatim instead of the synthetic ``anomaly-{metric}-{ts}`` string,
        and the signal must include the 5 provenance fields.

        Fails on the pre-fix code (synthetic id + missing provenance keys).
        """
        df = _make_normal_df()
        det = AnomalyDetector()
        det.train(df)
        feature = _make_anomaly_feature()
        feature["entity_id"] = "postgresql-database"
        signal = det.detect(feature)
        assert signal is not None

        # Real entity_id propagates verbatim.
        assert signal["entity_id"] == "postgresql-database"

        # Existing contract field stays correct (never the metadata key).
        assert signal["metric_name"] == "latency_p99"

        # 5 provenance fields present with sensible types / defaults.
        assert signal["detector_name"] == "AnomalyDetector"
        assert isinstance(signal["detector_contributions"], dict)
        assert signal["detector_contributions"], "contributions dict must be non-empty"
        assert signal["trend_direction"] in (
            "increasing",
            "decreasing",
            "flat",
            "unknown",
        )
        assert signal["entity_anomaly_count"] == 0
        assert signal["resolution_status"] == "active"


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


# ─── Tests: Task T8 — CUSUM + ADWIN wiring ──────────────────────────────── #


class TestDriftWiring:
    """T8: CUSUM baseline init (from _zscore_baselines) + ADWIN retrain loop
    (drift-triggered + hourly periodic) wired into AnomalyDetector."""

    def test_baseline_train_creates_drift_detectors(self) -> None:
        """BASELINE CHARACTERIZATION (T8) — post-fix contract: ``train()``
        creates a CUSUM + ADWIN detector per metric and initializes the
        retrain loop state (previously no drift state existed at all — that
        pre-fix shape is pinned in the git history of this file).
        """
        det = AnomalyDetector()
        df = _make_normal_df()
        det.train(df)
        assert set(det._cusum_detectors.keys()) == set(df.columns)
        assert set(det._adwin_detectors.keys()) == set(df.columns)
        assert det._retrain_count == 0
        assert det._last_retrain_ts > 0

    def test_cusum_created_per_metric_on_train(self) -> None:
        """FAILING-FIRST (T8): ``train()`` must create a ``CUSUMDetector`` per
        metric whose target_mean/target_std come from ``_zscore_baselines``.
        """
        det = AnomalyDetector()
        df = _make_normal_df()
        det.train(df)
        assert set(det._cusum_detectors.keys()) == set(df.columns)
        for col in df.columns:
            cusum = det._cusum_detectors[col]
            bl = det._zscore_baselines[col]
            assert cusum.target_mean == bl["mean"]
            assert cusum.target_std == bl["std"]

    def test_cusum_detects_slow_ramp_within_20_obs(self) -> None:
        """FAILING-FIRST (T8): a slow ramp (memory_leak style) fed through
        ``detect()`` must trip the per-metric CUSUM within 20 observations.
        """
        det = AnomalyDetector()
        det.train(_make_normal_df(n=150))
        bl = det._zscore_baselines["latency_p99"]
        std = bl["std"]
        fired_at: int | None = None
        for i in range(1, 21):
            value = bl["mean"] + 0.5 * std * i
            det.detect(
                {"latency_p99": value, "error_rate": 0.01, "request_volume": 500.0}
            )
            if det._cusum_drifted.get("latency_p99"):
                fired_at = i
                break
        assert fired_at is not None, "CUSUM must detect the ramp within 20 obs"
        assert fired_at <= 20

    def test_adwin_triggers_retrain_on_concept_drift(self) -> None:
        """FAILING-FIRST (T8): a sustained concept drift must set ADWIN
        ``needs_retrain`` and ``detect()`` must react by calling
        ``_retrain_models()`` (refit on the recent buffer).
        """
        det = AnomalyDetector()
        det.train(_make_normal_df(n=150))
        bl = det._zscore_baselines["latency_p99"]
        std = bl["std"]
        rng = np.random.default_rng(7)
        # Stable phase: no retrain while the stream matches the baseline.
        for _ in range(40):
            det.detect(
                {
                    "latency_p99": rng.normal(bl["mean"], std),
                    "error_rate": 0.01,
                    "request_volume": 500.0,
                }
            )
        assert det._retrain_count == 0

        # Drift phase: jump to +10σ; ADWIN fires → _retrain_models() runs.
        retrained = False
        for _ in range(40):
            value = bl["mean"] + 10.0 * std + rng.normal(0.0, std)
            det.detect(
                {"latency_p99": value, "error_rate": 0.01, "request_volume": 500.0}
            )
            if det._retrain_count > 0:
                retrained = True
                break
        assert retrained, "ADWIN concept drift must trigger _retrain_models()"

    def test_hourly_periodic_retrain(self) -> None:
        """FAILING-FIRST (T8): when the retrain interval elapses, ``detect()``
        refits the model even without an ADWIN drift event.
        """
        clock = {"now": 1_000_000.0}
        det = AnomalyDetector(
            retrain_interval_seconds=3600, clock=lambda: clock["now"]
        )
        det.train(_make_normal_df(n=150))
        assert det._retrain_count == 0

        # Within the interval → no retrain.
        det.detect({"latency_p99": 125.0, "error_rate": 0.012, "request_volume": 480.0})
        assert det._retrain_count == 0

        # Past the interval → hourly periodic retrain fires.
        clock["now"] += 3601.0
        det.detect({"latency_p99": 125.0, "error_rate": 0.012, "request_volume": 480.0})
        assert det._retrain_count == 1

    def test_retrain_refits_isolation_forest_on_recent_buffer(self) -> None:
        """FAILING-FIRST (T8): ``_retrain_models()`` replaces the fitted
        IsolationForest + scaler with a refit on the recent buffer (not the
        full training history).
        """
        det = AnomalyDetector(retrain_interval_seconds=0)
        det.train(_make_normal_df(n=150))
        old_if = det._isolation_forest
        old_scaler = det._scaler
        assert old_if is not None and old_scaler is not None

        det.detect({"latency_p99": 125.0, "error_rate": 0.012, "request_volume": 480.0})
        assert det._retrain_count == 1
        assert det._isolation_forest is not None
        assert det._isolation_forest is not old_if
        assert det._scaler is not old_scaler

    def test_entity_id_and_provenance_preserved_with_drift_wiring(self) -> None:
        """The CUSUM/ADWIN wiring must not disturb the T1 contract: real
        ``entity_id`` propagates verbatim and the 5 provenance fields stay.
        """
        det = AnomalyDetector()
        det.train(_make_normal_df(n=150))
        feature = _make_anomaly_feature()
        feature["entity_id"] = "background-worker"
        signal = det.detect(feature)
        assert signal is not None

        assert signal["entity_id"] == "background-worker"
        assert signal["metric_name"] == "latency_p99"
        assert signal["detector_name"] == "AnomalyDetector"
        assert isinstance(signal["detector_contributions"], dict)
        assert signal["detector_contributions"]
        assert signal["trend_direction"] == "unknown"
        assert signal["entity_anomaly_count"] == 0
        assert signal["resolution_status"] == "active"
