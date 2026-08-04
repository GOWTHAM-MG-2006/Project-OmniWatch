"""
OmniWatch — Predictive Intelligence Layer
Component: Robust Seasonal Detector Tests
Phase: 6
Purpose: Verify auto period detection, median-based robust decomposition,
         irregular-timestamp resampling, and <2-cycle graceful degradation
Inputs: N/A (test file)
Outputs: Test results
"""

import numpy as np
import pandas as pd
import pytest

from predictive.seasonal import RobustSeasonalDetector


# ------------------------------------------------------------------
# Synthetic data helpers
# ------------------------------------------------------------------

def _daily_series(n_cycles=3, period=288, seed=0, spike_index=None, spike_value=1000.0):
    """5-minute synthetic series with a strong daily (period=288) pattern.

    Base level 100, daily wave amplitude 20, small Gaussian noise.  An
    optional single-point outlier spike can be injected for robustness tests.
    """
    rng = np.random.default_rng(seed)
    n = period * n_cycles
    t = np.arange(n, dtype=float)
    base = np.sin(2.0 * np.pi * t / period)
    values = 100.0 + 20.0 * base + rng.normal(0.0, 1.0, n)
    if spike_index is not None:
        values[spike_index] = spike_value
    timestamps = pd.date_range("2026-01-01", periods=n, freq="5min")
    return timestamps, values


# ------------------------------------------------------------------
# Auto period detection
# ------------------------------------------------------------------

class TestAutoPeriodDetection:
    def test_picks_daily_period_from_synthetic_series(self):
        """A synthetic daily wave must yield period 288 (stronger than 12)."""
        ts, values = _daily_series(n_cycles=3, period=288)
        det = RobustSeasonalDetector()
        det.fit(ts, values)
        assert det.period == 288

    def test_picks_hourly_period_when_that_is_the_dominant_cycle(self):
        """A synthetic hourly wave must yield period 12."""
        rng = np.random.default_rng(3)
        period = 12
        n = period * 20  # 20 full hourly cycles
        t = np.arange(n, dtype=float)
        base = np.sin(2.0 * np.pi * t / period)
        values = 50.0 + 10.0 * base + rng.normal(0.0, 0.5, n)
        ts = pd.date_range("2026-01-01", periods=n, freq="5min")
        det = RobustSeasonalDetector()
        det.fit(ts, values)
        assert det.period == 12


# ------------------------------------------------------------------
# Robust median-based decomposition
# ------------------------------------------------------------------

class TestRobustDecomposition:
    def test_outlier_spike_does_not_distort_seasonal_baseline(self):
        """A single huge spike must not pull the median seasonal value."""
        ts, values = _daily_series(n_cycles=3, period=288, spike_index=100, spike_value=1000.0)
        det = RobustSeasonalDetector()
        det.fit(ts, values)

        bucket = 100 % 288
        expected = 100.0 + 20.0 * np.sin(2.0 * np.pi * 100.0 / 288.0)
        comp = det.seasonal_component
        assert comp is not None
        # Median of [spike, ~expected, ~expected] stays near expected.
        assert abs(comp[bucket] - expected) < 5.0

    def test_predict_returns_expected_seasonal_value(self):
        """predict() at a timestamp extrapolated one full period ahead ≈ bucket 0."""
        ts, values = _daily_series(n_cycles=3, period=288)
        det = RobustSeasonalDetector()
        det.fit(ts, values)
        future = ts[0] + pd.Timedelta(minutes=5 * 288)  # one full period ahead
        expected = 100.0 + 20.0 * np.sin(0.0)  # bucket 0
        assert abs(det.predict(future) - expected) < 5.0


# ------------------------------------------------------------------
# Irregular timestamp resampling
# ------------------------------------------------------------------

class TestResampling:
    def test_irregular_timestamps_resampled_to_regular_5min_grid(self):
        """Jittered timestamps must be regularised to exact 5-minute bins."""
        ts, values = _daily_series(n_cycles=3, period=288)
        rng = np.random.default_rng(7)
        jitter = pd.to_timedelta(rng.integers(-60, 60, size=len(ts)), unit="s")
        irregular_ts = ts + jitter

        det = RobustSeasonalDetector()
        det.fit(irregular_ts, values)

        idx = det.resampled_index
        assert len(idx) > 0
        # Timedelta comparison is resolution-agnostic (pandas 3.0 may store
        # the grid index in µs rather than ns internally).
        gaps = idx.to_series().diff().dropna()
        assert np.all(gaps == pd.Timedelta("5min"))


# ------------------------------------------------------------------
# Graceful degradation (< 2 full cycles)
# ------------------------------------------------------------------

class TestDegradation:
    def test_under_two_cycles_returns_flat_baseline(self):
        """Fewer than 2 full cycles of the smallest candidate period (12)
        must not raise; predict() returns the flat median baseline."""
        n = 12  # exactly one cycle of period 12 → < 2 cycles
        ts = pd.date_range("2026-01-01", periods=n, freq="5min")
        values = np.arange(n, dtype=float)

        det = RobustSeasonalDetector()
        det.fit(ts, values)  # must not raise

        assert det.period is None
        assert det.seasonal_component is None
        pred = det.predict(ts[0])
        assert np.isfinite(pred)
        assert abs(pred - float(np.median(values))) < 1e-9

    def test_predict_before_fit_raises(self):
        """Calling predict() before fit() is a usage error, not silent."""
        det = RobustSeasonalDetector()
        with pytest.raises(RuntimeError):
            det.predict(pd.Timestamp("2026-01-01"))
