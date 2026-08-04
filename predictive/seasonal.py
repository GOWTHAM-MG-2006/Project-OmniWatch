"""
OmniWatch — Predictive Intelligence Layer
Component: Robust Seasonal Detector
Phase: 6
Purpose: Robust seasonal decomposition with auto period detection and a
         median-based (outlier-resistant) seasonal baseline
Inputs: Raw (possibly irregular) (timestamps, values) series — 5-minute
        cloud-metric telemetry
Outputs: Fitted seasonal baseline: predict(timestamp) -> float, plus
         seasonal_component / seasonal_series accessors for the detector
         pipeline
"""

from __future__ import annotations

from typing import List, Optional, Sequence, Union

import numpy as np
import pandas as pd


# ─── Constants ────────────────────────────────────────────────────────────── #

# Candidate seasonal periods on a 5-minute grid:
#   12   = 1 hour
#   288  = 1 day
#   2016 = 1 week
_DEFAULT_CANDIDATE_PERIODS = (12, 288, 2016)
_DEFAULT_RESAMPLE_FREQ = "5min"

# pandas frequency -> step duration in seconds (for bucket mapping in predict()).
_FREQ_STEP_SECONDS = {
    "5min": 300,
    "5T": 300,
    "min": 60,
    "T": 60,
    "H": 3600,
    "h": 3600,
    "D": 86400,
}


# ─── Helpers ──────────────────────────────────────────────────────────────── #

def _step_seconds_for_freq(freq: str) -> int:
    """Return the per-bucket step duration in seconds for a pandas frequency."""
    normalized = freq.strip().lower()
    # pandas freq may be composed like "5min" — handle the common aliases.
    if normalized in _FREQ_STEP_SECONDS:
        return _FREQ_STEP_SECONDS[normalized]
    for alias, seconds in _FREQ_STEP_SECONDS.items():
        if normalized.endswith(alias):
            return seconds
    raise ValueError(f"unsupported resample frequency: {freq!r}")


def _autocorr(values: np.ndarray, lag: int) -> float:
    """Pearson autocorrelation of *values* at the given *lag*.

    Returns 0.0 when the lag is infeasible or either slice is constant
    (correlation undefined).  NaN inputs must be pre-dropped by the caller.
    """
    n = int(values.size)
    if n <= lag:
        return 0.0
    a = values[lag:]
    b = values[: n - lag]
    if a.std() == 0.0 or b.std() == 0.0:
        return 0.0
    return float(np.corrcoef(a, b)[0, 1])


def _coerce_timestamps(timestamps: Sequence) -> pd.DatetimeIndex:
    """Build a sorted DatetimeIndex from any timestamp-like input."""
    idx = pd.DatetimeIndex(pd.to_datetime(list(timestamps)))
    if idx.isna().any():
        raise ValueError("timestamps contain NaT values")
    return idx.sort_values()


# ─── RobustSeasonalDetector ───────────────────────────────────────────────── #

class RobustSeasonalDetector:
    """Seasonality detector producing an outlier-robust seasonal baseline.

    Pipeline:

    1. **Resample** — irregular input timestamps are aggregated onto a regular
       ``resample_freq`` (default 5-minute) grid via mean.
    2. **Auto period detection** — for every candidate period ``p`` with at
       least two full cycles of data, the lag-``p`` autocorrelation is
       computed; the candidate with the strongest autocorrelation wins.
    3. **Robust decomposition** — the seasonal component for each bucket is
       the **median** of the values sharing that phase, which keeps a single
       outlier spike from distorting the baseline.
    4. **Degradation** — when fewer than two full cycles of any candidate
       period are available, no period is chosen and ``predict()`` returns a
       flat median baseline instead of raising.

    The public API is intentionally small: ``fit(timestamps, values)``,
    ``predict(timestamp) -> float``, and the read-only ``seasonal_component``
    / ``seasonal_series`` / ``period`` / ``resampled_index`` accessors.
    """

    def __init__(
        self,
        candidate_periods: Sequence[int] = _DEFAULT_CANDIDATE_PERIODS,
        resample_freq: str = _DEFAULT_RESAMPLE_FREQ,
    ) -> None:
        self._candidate_periods: tuple = tuple(int(p) for p in candidate_periods)
        self._resample_freq = resample_freq
        self._step_seconds: int = _step_seconds_for_freq(resample_freq)

        # State set by fit().
        self._fitted = False
        self._period: Optional[int] = None
        self._seasonal: Optional[np.ndarray] = None  # shape (period,) when fitted
        self._flat_baseline: Optional[float] = None
        self._grid_start: Optional[pd.Timestamp] = None
        self._resampled_index: Optional[pd.DatetimeIndex] = None

    # ── public API ────────────────────────────────────────────────────── #

    def fit(
        self,
        timestamps: Sequence,
        values: Sequence[Union[int, float]],
    ) -> "RobustSeasonalDetector":
        """Fit the seasonal baseline to a (timestamps, values) series.

        Parameters
        ----------
        timestamps : sequence of datetime-like
            Observation times; may be irregular, unsorted, or carry NaNs
            (NaT values are rejected).
        values : sequence of float
            Observed metric values, positionally aligned with *timestamps*.

        Returns
        -------
        RobustSeasonalDetector
            ``self``, so fit can be chained.
        """
        values_arr = np.asarray(values, dtype=float)
        if values_arr.ndim != 1 or values_arr.size != len(timestamps):
            raise ValueError("timestamps and values must have equal length")
        if values_arr.size == 0:
            raise ValueError("cannot fit on empty data")

        idx = _coerce_timestamps(timestamps)

        series = pd.Series(values_arr, index=idx)
        resampled = series.resample(self._resample_freq).mean()
        resampled_index = resampled.index
        self._resampled_index = resampled_index
        first_ts = resampled_index[0]
        assert isinstance(first_ts, pd.Timestamp)
        self._grid_start = first_ts

        n_points = int(resampled.size)

        # ── auto period detection (only periods with >= 2 full cycles) ── #
        clean = resampled.dropna()
        best_period: Optional[int] = None
        best_score = -1.0
        for period in self._candidate_periods:
            if n_points < 2 * period:
                continue  # not enough data for this candidate
            score = _autocorr(clean.to_numpy(), period)
            if score > best_score:
                best_score = score
                best_period = period

        if best_period is None:
            # ── degrade: flat robust baseline, no period ────────────────── #
            self._period = None
            self._seasonal = None
            self._flat_baseline = (
                float(np.nanmedian(resampled.to_numpy())) if clean.size > 0 else 0.0
            )
            self._fitted = True
            return self

        # ── median-based robust decomposition ──────────────────────────── #
        self._period = best_period
        vals = resampled.to_numpy()
        seasonal = np.empty(best_period, dtype=float)
        for bucket in range(best_period):
            bucket_values = vals[bucket::best_period]
            if bucket_values.size == 0:
                seasonal[bucket] = 0.0
            else:
                seasonal[bucket] = float(np.nanmedian(bucket_values))
        self._seasonal = seasonal
        self._flat_baseline = None
        self._fitted = True
        return self

    def predict(self, timestamp) -> float:
        """Return the expected seasonal value at *timestamp*.

        The timestamp's position is mapped onto the resampled grid; its phase
        (``steps mod period``) selects the seasonal bucket.  Timestamps before
        the grid start or beyond the grid end extrapolate by phase, so
        forecasting a full period ahead yields the matching bucket.

        When the detector degraded (< 2 full cycles), the flat median baseline
        is returned for every timestamp.
        """
        if not self._fitted:
            raise RuntimeError("fit() must be called before predict()")

        if self._period is None:
            assert self._flat_baseline is not None
            return self._flat_baseline

        assert self._seasonal is not None
        assert self._grid_start is not None
        ts = pd.Timestamp(timestamp)
        delta_seconds = (ts - self._grid_start).total_seconds()
        steps = int(delta_seconds // self._step_seconds)
        bucket = steps % self._period
        return float(self._seasonal[bucket])

    # ── accessors ─────────────────────────────────────────────────────── #

    @property
    def is_fitted(self) -> bool:
        return self._fitted

    @property
    def period(self) -> Optional[int]:
        """Detected seasonal period, or None when degraded (< 2 cycles)."""
        return self._period

    @property
    def seasonal_component(self) -> Optional[List[float]]:
        """Per-bucket median seasonal values of length ``period``.

        ``None`` when the detector degraded to a flat baseline.
        """
        if self._seasonal is None:
            return None
        return self._seasonal.tolist()

    @property
    def seasonal_series(self) -> Optional[pd.Series]:
        """Seasonal component aligned to the resampled 5-minute grid.

        Repeats the per-bucket component across the full fitted index so it
        can be subtracted from / compared against the observed series.
        ``None`` when the detector degraded to a flat baseline.
        """
        if self._seasonal is None or self._resampled_index is None or self._period is None:
            return None
        period = self._period
        n = len(self._resampled_index)
        tiled = np.tile(self._seasonal, int(np.ceil(n / period)))[:n]
        return pd.Series(tiled, index=self._resampled_index, name="seasonal")

    @property
    def resampled_index(self) -> Optional[pd.DatetimeIndex]:
        """The regular grid the input series was resampled onto."""
        return self._resampled_index
