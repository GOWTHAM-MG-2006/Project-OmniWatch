"""
OmniWatch — Predictive Intelligence Layer
Component: Bayesian Fusion Engine
Phase: 6
Purpose: Fuse multiple detector anomaly scores into one calibrated probability
         using Platt-scaled LogisticRegression, with a cold-start confidence
         wrapper that down-weights early predictions.
Inputs: Detector score dicts {detector_name: score} + binary labels (fit)
Outputs: Calibrated anomaly probability in [0.0, 1.0] (predict)
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sklearn.linear_model import LogisticRegression


# ─── Constants ────────────────────────────────────────────────────────────── #

# Minimum samples required before a Platt calibration is attempted.  Below
# this (or when the labels collapse to a single class) the engine falls back
# to a weighted mean of the raw detector scores.
_MIN_FIT_SAMPLES = 2

#: Cold-start window for confidence scaling (see ColdStartAwareFusion).
_COLD_START_WINDOW = 100


# ─── Helpers ──────────────────────────────────────────────────────────────── #

def _clamp(value: float) -> float:
    """Clamp NaN / inf / out-of-range values into [0.0, 1.0]."""
    if math.isnan(value) or math.isinf(value):
        return 0.0
    return min(max(value, 0.0), 1.0)


def _sigmoid(logit: float) -> float:
    """Numerically stable sigmoid."""
    if logit >= 0:
        return 1.0 / (1.0 + math.exp(-logit))
    exp = math.exp(logit)
    return exp / (1.0 + exp)


# ─── BayesianFusionEngine ────────────────────────────────────────────────── #

class BayesianFusionEngine:
    """Fuse multiple detector anomaly scores into one calibrated probability.

    The engine treats each detector's raw anomaly score as a feature and fits
    a Platt-scaled ``LogisticRegression`` (sklearn) mapping those features to
    a binary anomaly label.  At prediction time the raw logit is scaled by
    ``platt_temperature`` before the sigmoid, so temperature controls how
    confident (extreme) the output probabilities are.

    **Cold start:**  When fewer than ``_MIN_FIT_SAMPLES`` samples have been
    seen, or the labels collapse to a single class, the model cannot be fit.
    In that state ``predict()`` falls back to ``_fallback_fuse`` — a weighted
    mean of the present detector scores (equal weights by default).
    """

    def __init__(
        self,
        detector_order: Sequence[str],
        platt_temperature: float = 1.0,
        detector_weights: Optional[Dict[str, float]] = None,
    ) -> None:
        """Initialise the fusion engine.

        Parameters
        ----------
        detector_order : sequence of str
            Ordered detector names.  The order is preserved and defines the
            feature-vector layout used for calibration.
        platt_temperature : float
            Scales the raw logit before the sigmoid (``logit / T``).  Higher
            values pull probabilities toward 0.5.
        detector_weights : dict[str, float] | None
            Optional per-detector weights for the cold-start fallback.  When
            ``None`` (or a detector is absent) equal weight is used.
        """
        self._detector_order: List[str] = list(detector_order)
        self._platt_temperature: float = float(platt_temperature)
        self._weights: Dict[str, float] = dict(detector_weights or {})

        # ── calibration state (set by fit()) ────────────────────────── #
        self._clf: Optional[LogisticRegression] = None
        self._fitted: bool = False
        self._n_samples: int = 0

    # ── public API ──────────────────────────────────────────────────── #

    @property
    def n_samples(self) -> int:
        """Number of samples seen by the most recent ``fit()`` call."""
        return self._n_samples

    @property
    def fitted(self) -> bool:
        """Whether a calibration model is currently available."""
        return self._fitted

    def fit(
        self, samples: Sequence[Tuple[Dict[str, float], int]]
    ) -> "BayesianFusionEngine":
        """Fit the Platt calibration on labelled detector-score samples.

        Parameters
        ----------
        samples : sequence of (detector_scores_dict, label)
            Each item is a pair of a ``{detector_name: score}`` dict and a
            binary label (``0`` = normal, ``1`` = anomaly).

        Returns
        -------
        BayesianFusionEngine
            ``self`` for chaining.
        """
        self._n_samples = len(samples)

        if self._n_samples < _MIN_FIT_SAMPLES:
            self._fitted = False
            return self

        X: List[List[float]] = []
        y: List[int] = []
        for scores, label in samples:
            X.append(self._feature_vector(scores))
            y.append(int(label))

        # LogisticRegression needs both classes present to fit.
        if len(set(y)) < 2:
            self._fitted = False
            return self

        clf = LogisticRegression(solver="lbfgs", max_iter=1000)
        clf.fit(X, y)
        self._clf = clf
        self._fitted = True
        return self

    def predict(self, detector_scores_dict: Dict[str, float]) -> float:
        """Return a calibrated anomaly probability in [0.0, 1.0].

        Parameters
        ----------
        detector_scores_dict : dict[str, float]
            ``{detector_name: raw_score}``.  Detectors absent from the dict
            are treated as ``0.0``.

        Returns
        -------
        float
            Calibrated probability in ``[0.0, 1.0]``.  Falls back to a
            weighted mean of the raw scores when the engine is unfitted.
        """
        if not self._fitted or self._clf is None:
            return self._fallback_fuse(detector_scores_dict)

        logit = float(
            self._clf.decision_function([self._feature_vector(detector_scores_dict)])[0]
        )
        scaled_logit = logit / self._platt_temperature
        return _clamp(_sigmoid(scaled_logit))

    # ── internals ───────────────────────────────────────────────────── #

    def _feature_vector(self, scores: Dict[str, float]) -> List[float]:
        """Build the ordered feature vector from a scores dict."""
        return [float(scores.get(detector, 0.0)) for detector in self._detector_order]

    def _fallback_fuse(self, scores: Dict[str, float]) -> float:
        """Weighted mean over the full detector order (cold-start fallback).

        Every detector in ``_detector_order`` participates in the mean: a
        detector absent from *scores* (or carrying ``None``) contributes
        ``0.0``, so a detector that did not fire cannot inflate the fused
        score.  Weights default to ``1.0`` per detector unless overridden via
        ``detector_weights``.
        """
        acc = 0.0
        total_weight = 0.0
        for detector in self._detector_order:
            value = scores.get(detector)
            if value is None:
                value = 0.0
            weight = self._weights.get(detector, 1.0)
            acc += weight * float(value)
            total_weight += weight
        if total_weight == 0.0:
            return 0.0
        return _clamp(acc / total_weight)


# ─── ColdStartAwareFusion ────────────────────────────────────────────────── #

class ColdStartAwareFusion:
    """Wrap a :class:`BayesianFusionEngine` with cold-start confidence scaling.

    Early predictions are inherently less trustworthy because the calibration
    model has seen few samples.  This wrapper exposes a ``confidence`` value
    that scales linearly from ``0.0`` (no samples) to ``1.0`` once
    ``n_samples >= 100``, so downstream consumers can down-weight cold-start
    predictions.
    """

    def __init__(
        self,
        engine: BayesianFusionEngine,
        cold_start_window: int = _COLD_START_WINDOW,
    ) -> None:
        """Wrap *engine*.

        Parameters
        ----------
        engine : BayesianFusionEngine
            The underlying fusion engine to delegate to.
        cold_start_window : int
            Number of samples at which confidence reaches ``1.0``.
        """
        self._engine = engine
        self._cold_start_window = float(cold_start_window)

    # ── public API ──────────────────────────────────────────────────── #

    @property
    def n_samples(self) -> int:
        """Number of samples seen by the wrapped engine."""
        return self._engine.n_samples

    @property
    def confidence(self) -> float:
        """Cold-start confidence in ``[0.0, 1.0]``.

        Computed as ``min(1.0, n_samples / cold_start_window)``.
        """
        return min(1.0, self.n_samples / self._cold_start_window)

    def fit(
        self, samples: Sequence[Tuple[Dict[str, float], int]]
    ) -> "ColdStartAwareFusion":
        """Fit the wrapped engine (see :meth:`BayesianFusionEngine.fit`)."""
        self._engine.fit(samples)
        return self

    def predict(self, detector_scores_dict: Dict[str, float]) -> float:
        """Delegate to the wrapped engine's calibrated probability."""
        return self._engine.predict(detector_scores_dict)