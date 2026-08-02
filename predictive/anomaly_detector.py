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
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import joblib
import numpy as np
import pandas as pd
import yaml
from scipy import stats
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from predictive.config.settings import Settings


# ─── Constants ────────────────────────────────────────────────────────────── #

_COLD_START_MIN_SAMPLES = 100
_DEFAULT_ZSCORE_THRESHOLD = 3.0
_SEASONAL_DEFAULT_PERIOD = 24


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

    def __init__(self, settings: Settings | None = None) -> None:
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

    def detect(self, feature: Dict[str, float]) -> Optional[Dict[str, Any]]:
        """Run anomaly detection on a single feature vector.

        Parameters
        ----------
        feature : dict
            ``{metric_name: value}`` — a single observation.

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

        for metric, value in feature.items():
            value = float(value)
            if metric not in self._feature_cols:
                continue

            # ── Z-Score component ───────────────────────────────────── #
            baseline = self._zscore_baselines.get(metric, {"mean": 0.0, "std": 1.0})
            std = baseline["std"] if baseline["std"] > 0 else 1.0
            z = abs(value - baseline["mean"]) / std
            z_score_normalised = _sigmoid_normalise(z)
            scores.append(z_score_normalised)
            deviations.append(z - _DEFAULT_ZSCORE_THRESHOLD)

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

        # ── AnomalySignal ───────────────────────────────────────────── #
        # Use the first metric as primary entity reference
        primary_metric = list(feature.keys())[0] if feature else "unknown"

        signal: Dict[str, Any] = {
            "entity_id": f"anomaly-{primary_metric}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
            "entity_type": "API_NODE",
            "metric_name": primary_metric,
            "anomaly_score": round(anomaly_score, 4),
            "confidence": round(confidence, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "deviation_from_baseline": round(deviation, 4),
            "source_type": source_type,
        }
        return signal

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
