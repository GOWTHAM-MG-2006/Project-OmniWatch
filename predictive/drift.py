"""
OmniWatch — Predictive Intelligence Layer
Component: Drift Detection (CUSUM + ADWIN)
Phase: 6
Purpose: Streaming drift detectors for online anomaly-detection baselines
Inputs: Scalar metric values from the feature stream
Outputs: Drift flags and retrain signals for the predictive layer
"""

from __future__ import annotations

import math

__all__ = ["CUSUMDetector", "ADWINDriftDetector"]


class CUSUMDetector:
    """Cumulative-sum (CUSUM) drift detector for sustained mean shifts.

    Maintains two one-sided cumulative sums in normalized units, ``s_pos`` and
    ``s_neg``.  A single observation's deviation is
    ``(value - target_mean) / target_std``.  The positive sum accumulates
    upward deviations minus ``slack``; the negative sum accumulates downward
    deviations minus ``slack``.  Drift is reported when either sum exceeds
    ``drift_threshold``.

    Standard CUSUM recursion::

        s_pos = max(0, s_pos + deviation - slack)
        s_neg = max(0, s_neg - deviation - slack)

    Args:
        target_mean: Baseline mean, supplied by the caller (e.g. from a
            Welford baseline computed during normal operation).
        target_std: Baseline standard deviation.  If zero, any deviation from
            ``target_mean`` is treated as instantaneous drift.
        drift_threshold: Alarm threshold in normalized units.
        slack: Allowable noise band in normalized units; values within
            ``slack`` of the target do not accumulate.
    """

    def __init__(
        self,
        target_mean: float,
        target_std: float,
        drift_threshold: float = 4.0,
        slack: float = 0.5,
    ) -> None:
        self.target_mean = float(target_mean)
        self.target_std = float(target_std)
        if drift_threshold <= 0:
            raise ValueError("drift_threshold must be positive")
        if slack < 0:
            raise ValueError("slack must be non-negative")
        self.drift_threshold = float(drift_threshold)
        self.slack = float(slack)
        self.s_pos = 0.0
        self.s_neg = 0.0

    def _deviation(self, value: float) -> float:
        """Normalized one-sample deviation from the target mean."""
        diff = value - self.target_mean
        if self.target_std <= 0:
            if diff > 0:
                return math.inf
            if diff < 0:
                return -math.inf
            return 0.0
        return diff / self.target_std

    def update(self, value: float) -> bool:
        """Ingest one observation; returns True when drift is detected.

        Once a sum crosses the threshold it stays elevated (and update keeps
        returning True) until :meth:`reset` is called.
        """
        dev = self._deviation(value)
        self.s_pos = max(0.0, self.s_pos + dev - self.slack)
        self.s_neg = max(0.0, self.s_neg - dev - self.slack)
        return self.s_pos > self.drift_threshold or self.s_neg > self.drift_threshold

    def reset(self) -> None:
        """Zero both cumulative sums, clearing any detected drift."""
        self.s_pos = 0.0
        self.s_neg = 0.0


class _Bucket:
    """ADWIN window bucket: power-of-two sized batch of observations.

    ``total`` is the sum of the values; ``variance`` is the sum of squared
    deviations from this bucket's mean.
    """

    __slots__ = ("size", "total", "variance")

    def __init__(self, size: int = 1, total: float = 0.0, variance: float = 0.0) -> None:
        self.size = size
        self.total = total
        self.variance = variance

    @property
    def mean(self) -> float:
        return self.total / self.size if self.size else 0.0


class ADWINDriftDetector:
    """Adaptive Windowing (ADWIN) concept-drift detector.

    Maintains a compressed window of buckets whose sizes are powers of two.
    After every new observation the Hoeffding bound is applied to every
    possible cut of the window: if the means of the older and newer
    sub-windows differ by more than the bound, the older data is dropped and
    concept drift is reported.

    Drift checks are disabled until the window holds at least
    ``min_window_size`` observations, so short streams cannot trigger false
    alarms.  A detected drift flips :attr:`needs_retrain` to True until
    :meth:`reset` is called.

    Args:
        delta: Confidence parameter for the Hoeffding bound (0 < delta < 1).
            Smaller values demand stronger evidence before declaring drift.
        min_window_size: Minimum window length before drift checking starts.
        max_buckets: Maximum buckets kept per size level before compression.
    """

    def __init__(
        self,
        delta: float = 0.002,
        min_window_size: int = 30,
        max_buckets: int = 5,
    ) -> None:
        if not 0.0 < delta < 1.0:
            raise ValueError("delta must be in (0, 1)")
        if min_window_size < 1:
            raise ValueError("min_window_size must be >= 1")
        self.delta = float(delta)
        self.min_window_size = int(min_window_size)
        self._max_buckets = int(max_buckets)
        self.reset()

    @property
    def needs_retrain(self) -> bool:
        """True after a drift event until :meth:`reset` is called."""
        return self._needs_retrain

    @property
    def window_size(self) -> int:
        """Number of observations currently buffered in the window."""
        return self._n

    def update(self, value: float) -> bool:
        """Ingest one observation; returns True when concept drift is detected."""
        self._add(value)
        if self._n >= self.min_window_size and self._check_drift():
            self._needs_retrain = True
            return True
        return False

    def reset(self) -> None:
        """Clear the window and any pending retrain signal."""
        self._buckets: list[_Bucket] = []
        self._total = 0.0
        self._variance = 0.0
        self._n = 0
        self._needs_retrain = False

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _add(self, value: float) -> None:
        """Append a size-1 bucket and compress the window."""
        value = float(value)
        self._buckets.append(_Bucket(size=1, total=value, variance=0.0))
        self._n += 1
        self._total += value
        self._compress()

    def _compress(self) -> None:
        """Merge equal-size buckets that exceed ``max_buckets`` per level.

        Merging preserves the window's total sum and total variance, so drift
        checks over the compressed representation stay exact.
        """
        buckets = self._buckets
        n = len(buckets)
        j = 0
        while j < n:
            size = buckets[j].size
            k = j
            while k < n and buckets[k].size == size:
                k += 1
            if k - j > self._max_buckets:
                b1 = buckets[j]
                b2 = buckets[j + 1]
                merged = _Bucket(
                    size=b1.size + b2.size,
                    total=b1.total + b2.total,
                    variance=(
                        b1.variance
                        + b2.variance
                        + (b1.size * (b1.mean - b2.mean) ** 2) / 2.0
                    ),
                )
                buckets[j : j + 2] = [merged]
                n = len(buckets)
                j = 0
            else:
                j = k

    def _check_drift(self) -> bool:
        """Test every window cut against the Hoeffding bound; shrink on drift.

        Returns True if at least one shrink occurred.  After dropping old
        buckets the remaining window is re-tested until it is stable, mirroring
        the classic ADWIN loop.
        """
        changed = True
        shrink = False
        while changed:
            changed = False
            buckets = self._buckets
            n = len(buckets)
            if n < 2:
                break

            pref_size = [0] * n
            pref_total = [0.0] * n
            acc_size = 0
            acc_total = 0.0
            for i, b in enumerate(buckets):
                acc_size += b.size
                acc_total += b.total
                pref_size[i] = acc_size
                pref_total[i] = acc_total

            window_n = self._n
            window_total = self._total
            for cut in range(n - 1):
                n0 = pref_size[cut]
                n1 = window_n - n0
                if n1 == 0:
                    continue
                mean0 = pref_total[cut] / n0
                mean1 = (window_total - pref_total[cut]) / n1
                m = (n0 * n1) / window_n
                eps = math.sqrt(
                    (1.0 / (2.0 * m)) * math.log(4.0 * window_n / self.delta)
                )
                if abs(mean0 - mean1) > eps:
                    del buckets[: cut + 1]
                    self._n = 0
                    self._total = 0.0
                    self._variance = 0.0
                    for b in buckets:
                        self._n += b.size
                        self._total += b.total
                        self._variance += b.variance
                    shrink = True
                    changed = True
                    break
        return shrink
