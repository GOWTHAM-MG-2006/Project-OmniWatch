"""
OmniWatch — Predictive Intelligence Layer
Component: Anomaly Detector
Phase: 6
Purpose: IsolationForest/Z-Score/Seasonal anomaly detection wrapper
Inputs: Feature vectors from ClickHouse feature_vectors
Outputs: AnomalySignal dict or None
"""

from __future__ import annotations

import math
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable, Deque, Dict, List, Optional

import joblib
import numpy as np
import pandas as pd
import yaml
from scipy import stats
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from predictive.config.settings import Settings
from predictive.drift import ADWINDriftDetector, CUSUMDetector


# ─── Constants ────────────────────────────────────────────────────────────── #

_COLD_START_MIN_SAMPLES = 100
_DEFAULT_ZSCORE_THRESHOLD = 3.0
_SEASONAL_DEFAULT_PERIOD = 24

# T8 — drift detection & retrain loop
_CUSUM_DRIFT_THRESHOLD = 4.0
_CUSUM_SLACK = 0.5
_ADWIN_DELTA = 0.002
_ADWIN_MIN_WINDOW = 30
_ADWIN_MAX_BUCKETS = 5
# ADWIN input scaling: the Hoeffding bound at delta=0.002 is ~2.4 for a
# single-point cut (n1=1), so a lone ~2.6σ outlier would falsely fire.  Feeding
# the z-score scaled by 0.5 keeps normal σ~1 noise (and single outliers up to
# ~4.8σ) silent while a genuine sustained shift (e.g. 10σ) fires immediately.
_ADWIN_INPUT_SCALE = 0.5
_RETRAIN_BUFFER_MAX = 500
_DEFAULT_RETRAIN_INTERVAL_SECONDS = 3600


# ─── Helpers ──────────────────────────────────────────────────────────────── #

def _load_detection_rules(path: str | None = None) -> dict:
    """Load per-metric detection thresholds from YAML."""
    if path is None:
        path = os.path.join(os.path.dirname(__file__), "config", "detection_rules.yaml")
    if not os.path.isfile(path):
        return {"metrics": {}}
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {"metrics": {}}


def _clamp(value: float) -> float:
    """Clamp NaN / inf to 0.0."""
    if math.isnan(value) or math.isinf(value):
        return 0.0
    return value


def _sigmoid_normalise(zscore: float, threshold: float = _DEFAULT_ZSCORE_THRESHOLD) -> float:
    """Map a Z-score to [0, 1] via a sigmoid-like curve centred on *threshold*."""
    # When zscore == threshold → score ≈ 0.5; when zscore == 0 → score ≈ 0.0
    raw = 1.0 / (1.0 + math.exp(-zscore + threshold))
    return _clamp(raw)


# ─── AnomalyDetector ──────────────────────────────────────────────────────── #

class AnomalyDetector:
    """Multi-algorithm anomaly detection wrapper.

    Combines three detection strategies:

    1. **IsolationForest** (sklearn) — unsupervised tree-based detector.
       Active only after ≥100 training samples (cold-start guard).
    2. **Z-Score** — scipy-based statistical outlier scoring against running
       mean/std. Always active once trained.
    3. **Seasonal Naive** — predicts next value from the last seasonal cycle
       using statsmodels. Large residual → anomaly.

    After scoring, NaN/inf values are clamped to 0.0 and the final score is
    returned only when it exceeds the configured threshold
    (``predictive_anomaly_score_threshold``).
    """

    def __init__(
        self,
        settings: Settings | None = None,
        retrain_interval_seconds: int = _DEFAULT_RETRAIN_INTERVAL_SECONDS,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.settings = settings or Settings.from_env()
        self._rules = _load_detection_rules()
        self._threshold = self.settings.predictive_anomaly_score_threshold

        # ── state set by train() ────────────────────────────────────── #
        self._isolation_forest: Optional[IsolationForest] = None
        self._scaler: Optional[StandardScaler] = None
        self._train_count: int = 0

        # Per-metric Z-Score baselines: {metric_name: {"mean", "std"}}
        self._zscore_baselines: Dict[str, Dict[str, float]] = {}

        # Seasonal naive: last full cycle per metric {metric_name: [values]}
        self._seasonal_history: Dict[str, List[float]] = {}
        self._seasonal_period: int = self.settings.predictive_seasonality_period

        # ── T8: drift detection + retrain loop ──────────────────────── #
        self._retrain_interval_seconds: int = int(retrain_interval_seconds)
        self._clock: Callable[[], float] = clock or time.time

        # Per-metric CUSUM detectors (baselines from _zscore_baselines).
        self._cusum_detectors: Dict[str, CUSUMDetector] = {}
        self._cusum_drifted: Dict[str, bool] = {}

        # Per-metric ADWIN concept-drift detectors (drift → retrain).
        self._adwin_detectors: Dict[str, ADWINDriftDetector] = {}

        # Recent-observation buffer used by _retrain_models().
        self._recent_buffer: Deque[Dict[str, float]] = deque(maxlen=_RETRAIN_BUFFER_MAX)
        self._retrain_count: int = 0
        self._last_retrain_ts: float = 0.0

    # ── public API ──────────────────────────────────────────────────── #

    def train(self, features_df: pd.DataFrame) -> None:
        """Fit detection models on a DataFrame of feature vectors.

        Parameters
        ----------
        features_df : pd.DataFrame
            Rows = observations, columns = metric names.  The DataFrame may
            contain NaN values — they are dropped per-column before fitting.

        The method sets internal baselines, fits the IsolationForest (when
        enough samples exist), and prepares Z-Score / seasonal history data.
        """
        self._train_count = len(features_df)
        self._feature_cols = list(features_df.columns)

        # ── IsolationForest (cold-start guard) ──────────────────────── #
        if self._train_count >= _COLD_START_MIN_SAMPLES:
            clean = features_df.dropna()
            if len(clean) > 0:
                self._scaler = StandardScaler()
                scaled = self._scaler.fit_transform(clean.values)
                self._isolation_forest = IsolationForest(
                    n_estimators=100,
                    contamination=0.05,
                    random_state=42,
                )
                self._isolation_forest.fit(scaled)

        # ── Z-Score baselines ───────────────────────────────────────── #
        for col in features_df.columns:
            series = features_df[col].dropna()
            if len(series) > 0:
                self._zscore_baselines[col] = {
                    "mean": float(series.mean()),
                    "std": float(series.std()) if series.std() > 0 else 1.0,
                }
            else:
                self._zscore_baselines[col] = {"mean": 0.0, "std": 1.0}

        # ── Seasonal history ────────────────────────────────────────── #
        for col in features_df.columns:
            values = features_df[col].dropna().tolist()
            # Keep last full cycle (or all if shorter)
            self._seasonal_history[col] = values[-self._seasonal_period :]

        # ── T8: per-metric drift detectors (CUSUM + ADWIN) ──────────── #
        # CUSUM targets come straight from the Z-Score baselines just
        # computed above; ADWIN windows start fresh for this training run.
        self._cusum_detectors = {
            col: CUSUMDetector(
                target_mean=bl["mean"],
                target_std=bl["std"],
                drift_threshold=_CUSUM_DRIFT_THRESHOLD,
                slack=_CUSUM_SLACK,
            )
            for col, bl in self._zscore_baselines.items()
        }
        self._cusum_drifted = {}
        self._adwin_detectors = {
            col: ADWINDriftDetector(
                delta=_ADWIN_DELTA,
                min_window_size=_ADWIN_MIN_WINDOW,
                max_buckets=_ADWIN_MAX_BUCKETS,
            )
            for col in features_df.columns
        }
        # Fresh retrain timer + observation buffer for this training run.
        self._last_retrain_ts = self._clock()
        self._recent_buffer.clear()

    def detect(self, feature: Dict[str, float]) -> Optional[Dict[str, Any]]:
        """Run anomaly detection on a single feature vector.

        Parameters
        ----------
        feature : dict
            ``{metric_name: value}`` — a single observation.  May optionally
            carry ``entity_id`` (from the entity-resolution layer) and
            ``trend_direction``; both propagate into the signal when present.
            Metadata keys are never treated as scoring metrics.

        Returns
        -------
        dict | None
            An ``AnomalySignal`` dict when the composite score exceeds
            ``predictive_anomaly_score_threshold``, otherwise ``None``.
        """
        if not self._feature_cols:
            return None

        scores: List[float] = []
        deviations: List[float] = []
        contributions: Dict[str, float] = {}

        for metric, value in feature.items():
            if metric not in self._feature_cols:
                # Metadata keys (e.g. entity_id) are not scoring features.
                continue
            value = float(value)

            # ── Z-Score component ───────────────────────────────────── #
            baseline = self._zscore_baselines.get(metric, {"mean": 0.0, "std": 1.0})
            std = baseline["std"] if baseline["std"] > 0 else 1.0
            z = abs(value - baseline["mean"]) / std
            z_score_normalised = _sigmoid_normalise(z)
            scores.append(z_score_normalised)
            deviations.append(z - _DEFAULT_ZSCORE_THRESHOLD)
            contributions["z_score"] = z_score_normalised

            # ── Seasonal naive component ────────────────────────────── #
            history = self._seasonal_history.get(metric, [])
            if len(history) >= self._seasonal_period:
                seasonal_pred = history[-1]  # naive: last value = next prediction
                seasonal_resid = abs(value - seasonal_pred)
                seasonal_std = baseline["std"] if baseline["std"] > 0 else 1.0
                seasonal_z = seasonal_resid / seasonal_std
                seasonal_score = _sigmoid_normalise(seasonal_z)
                scores.append(seasonal_score)
                deviations.append(seasonal_z - _DEFAULT_ZSCORE_THRESHOLD)
                contributions["seasonal_naive"] = seasonal_score

            # ── T8: feed drift detectors (CUSUM + ADWIN) ────────────── #
            # CUSUM tracks the raw value against its baseline; ADWIN tracks
            # the scaled Z-score (see _ADWIN_INPUT_SCALE) so normal noise and
            # single outliers stay silent while sustained shifts fire.
            if math.isfinite(value):
                cusum = self._cusum_detectors.get(metric)
                if cusum is not None and cusum.update(value):
                    self._cusum_drifted[metric] = True
                adwin = self._adwin_detectors.get(metric)
                if adwin is not None and math.isfinite(z):
                    adwin.update(z * _ADWIN_INPUT_SCALE)

        # ── T8: recent-observation buffer + retrain trigger ─────────── #
        # Buffer the current observation (feature columns only) so a drift
        # retrain can refit on the recent window, then check whether ADWIN
        # fired or the periodic interval elapsed.
        obs = {m: float(v) for m, v in feature.items() if m in self._feature_cols}
        if obs:
            self._recent_buffer.append(obs)
        self._maybe_retrain()

        # ── IsolationForest component ───────────────────────────────── #
        if self._isolation_forest is not None and self._scaler is not None:
            vec = [feature.get(c, 0.0) for c in self._feature_cols]
            vec_clean = [v if not (math.isnan(v) or math.isinf(v)) else 0.0 for v in vec]
            scaled = self._scaler.transform([vec_clean])
            raw_score = self._isolation_forest.decision_function(scaled)[0]
            # decision_function: higher = more normal.  Invert & map to [0,1]
            iso_score = float(_clamp(1.0 / (1.0 + math.exp(raw_score))))
            scores.append(iso_score)
            deviations.append(-raw_score)  # positive raw → more normal → low deviation
            contributions["isolation_forest"] = iso_score

        if not scores:
            return None

        # ── Composite score ─────────────────────────────────────────── #
        anomaly_score = float(np.mean(scores))
        anomaly_score = _clamp(anomaly_score)

        # ── Threshold gate ──────────────────────────────────────────── #
        if anomaly_score < self._threshold:
            return None

        # ── Confidence ──────────────────────────────────────────────── #
        confidence = float(np.mean([s * 100.0 for s in scores]))
        confidence = _clamp(confidence)
        confidence = min(max(confidence, 0.0), 100.0)

        # ── Deviation ───────────────────────────────────────────────── #
        deviation = float(np.mean(deviations)) if deviations else 0.0
        deviation = _clamp(deviation)

        # ── Source type heuristic ────────────────────────────────────── #
        source_type = "security" if any(
            kw in str(metric) for kw in ("auth", "login", "access", "crypto")
        ) else "performance"

        # ── Entity / primary metric ─────────────────────────────────── #
        # entity_id comes from the feature vector (entity-resolution layer),
        # falling back to "unknown" when the feature carries none.
        entity_id = feature.get("entity_id", "unknown")

        # Primary metric = first real scoring feature (skip metadata keys).
        primary_metric = next(
            (k for k in feature if k in self._feature_cols),
            "unknown",
        )

        # ── Trend direction (provenance) ────────────────────────────── #
        _trend = feature.get("trend_direction")
        trend_direction = (
            _trend if _trend in ("increasing", "decreasing", "flat") else "unknown"
        )

        # ── AnomalySignal ───────────────────────────────────────────── #
        signal: Dict[str, Any] = {
            "entity_id": entity_id,
            "entity_type": "API_NODE",
            "metric_name": primary_metric,
            "anomaly_score": round(anomaly_score, 4),
            "confidence": round(confidence, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "deviation_from_baseline": round(deviation, 4),
            "source_type": source_type,
            # ── Provenance fields (Task T1) ────────────────────────── #
            "detector_name": self.__class__.__name__,
            "detector_contributions": contributions,
            "trend_direction": trend_direction,
            "entity_anomaly_count": 0,
            "resolution_status": "active",
        }
        return signal

    # ── T8: drift-driven retrain loop ──────────────────────────────────── #

    def _maybe_retrain(self) -> None:
        """Retrain when ADWIN reports concept drift or the periodic interval
        has elapsed (whichever comes first)."""
        elapsed = self._clock() - self._last_retrain_ts
        adwin_fired = any(d.needs_retrain for d in self._adwin_detectors.values())
        if adwin_fired or elapsed >= self._retrain_interval_seconds:
            self._retrain_models()

    def _retrain_models(self) -> None:
        """Refit baselines + IsolationForest on the recent-observation buffer.

        Runs after drift: recomputes the Z-Score baselines, rebuilds the CUSUM
        detectors from the new baselines, resets the ADWIN windows and refits
        the IsolationForest on the buffered observations (not the full
        training history).  No-op when the buffer is empty.
        """
        if not self._recent_buffer:
            return
        df = pd.DataFrame(list(self._recent_buffer))

        # ── Z-Score baselines from the recent window ────────────────── #
        for col in df.columns:
            series = df[col].dropna()
            if len(series) > 0:
                self._zscore_baselines[col] = {
                    "mean": float(series.mean()),
                    "std": float(series.std()) if series.std() > 0 else 1.0,
                }
            else:
                self._zscore_baselines[col] = {"mean": 0.0, "std": 1.0}

        # ── IsolationForest refit on the recent window ──────────────── #
        clean = df.dropna()
        if len(clean) > 0:
            self._scaler = StandardScaler()
            scaled = self._scaler.fit_transform(clean.values)
            self._isolation_forest = IsolationForest(
                n_estimators=100,
                contamination=0.05,
                random_state=42,
            )
            self._isolation_forest.fit(scaled)

        # ── Seasonal history from the recent window ─────────────────── #
        for col in df.columns:
            values = df[col].dropna().tolist()
            if values:
                self._seasonal_history[col] = values[-self._seasonal_period :]

        # ── Rebuild CUSUM from new baselines; reset ADWIN windows ───── #
        self._cusum_detectors = {
            col: CUSUMDetector(
                target_mean=bl["mean"],
                target_std=bl["std"],
                drift_threshold=_CUSUM_DRIFT_THRESHOLD,
                slack=_CUSUM_SLACK,
            )
            for col, bl in self._zscore_baselines.items()
        }
        self._cusum_drifted = {}
        for adwin in self._adwin_detectors.values():
            adwin.reset()

        self._retrain_count += 1
        self._last_retrain_ts = self._clock()

    # ── Model persistence ───────────────────────────────────────────── #

    def save_model(self, path: str) -> None:
        """Serialise trained state to disk via joblib."""
        state = {
            "isolation_forest": self._isolation_forest,
            "scaler": self._scaler,
            "train_count": self._train_count,
            "feature_cols": self._feature_cols,
            "zscore_baselines": self._zscore_baselines,
            "seasonal_history": self._seasonal_history,
            "seasonal_period": self._seasonal_period,
            "threshold": self._threshold,
        }
        joblib.dump(state, path)

    def load_model(self, path: str) -> None:
        """Restore trained state from a joblib serialised file."""
        state = joblib.load(path)
        self._isolation_forest = state["isolation_forest"]
        self._scaler = state["scaler"]
        self._train_count = state["train_count"]
        self._feature_cols = state["feature_cols"]
        self._zscore_baselines = state["zscore_baselines"]
        self._seasonal_history = state["seasonal_history"]
        self._seasonal_period = state["seasonal_period"]
        self._threshold = state["threshold"]

        # ── T8: rebuild drift detectors from the restored baselines ──── #
        # A loaded model behaves like a freshly trained one: CUSUM targets
        # come from the restored baselines, ADWIN windows start fresh, and
        # the retrain timer starts at load time (so the first observation
        # does not trigger an immediate periodic retrain).
        self._cusum_detectors = {
            col: CUSUMDetector(
                target_mean=bl["mean"],
                target_std=bl["std"],
                drift_threshold=_CUSUM_DRIFT_THRESHOLD,
                slack=_CUSUM_SLACK,
            )
            for col, bl in self._zscore_baselines.items()
        }
        self._cusum_drifted = {}
        self._adwin_detectors = {
            col: ADWINDriftDetector(
                delta=_ADWIN_DELTA,
                min_window_size=_ADWIN_MIN_WINDOW,
                max_buckets=_ADWIN_MAX_BUCKETS,
            )
            for col in self._feature_cols
        }
        self._last_retrain_ts = self._clock()
