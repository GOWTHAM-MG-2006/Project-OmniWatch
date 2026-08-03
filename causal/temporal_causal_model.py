"""
OmniWatch — Causal Graph Engine
Component: Temporal Causal Model
Phase: 7
Purpose: Lag-aware causal pre-processing for PyRCA.  Computes lagged
         cross-correlation between candidate cause series and effect
         series, keeps pairs whose |corr| >= correlation_threshold with
         significance p <= p_value_threshold, then builds a lag-aligned
         wide feature frame so the PC algorithm sees genuine temporal
         ordering (a cause temporally precedes its effect in the frame).
Inputs: Wide time-series DataFrame (datetime index, one column per KPI /
         entity metric) plus optional candidate / effect column lists.
Outputs: LaggedEdge records (cause, effect, lag, correlation, p_value)
         and a lag-aligned DataFrame consumable by
         PyRCAAdapter.discover_graph() / find_root_causes().
"""

from __future__ import annotations

import logging
import math
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from storage.common import StorageError, create_logger

_LOG: logging.Logger = create_logger("omniwatch.causal.temporal_causal_model")

_DEFAULT_RULES_PATH = Path(__file__).resolve().parent / "config" / "causal_rules.yaml"


@dataclass
class LaggedEdge:
    """A lag-aligned candidate causal edge found by the temporal model.

    ``cause.shift(lag)`` correlates with ``effect`` at ``correlation`` with
    significance ``p_value``.  ``lag`` is expressed in the same units as the
    frame index (minutes for the OmniWatch feature frames).
    """

    cause: str
    effect: str
    lag: int
    correlation: float
    p_value: float


def _column(frame: pd.DataFrame, name: str) -> pd.Series:
    """Extract one column as a float Series.

    Pyright cannot narrow ``frame[name]`` (untyped pandas: ``Series |
    DataFrame``), so we route the values through ``to_numpy()`` and rebuild a
    ``Series`` — the return type is then unambiguous.  A DataFrame result
    (duplicate column name) falls back to its first column.
    """
    raw = frame[name]
    if isinstance(raw, pd.DataFrame):
        raw = raw.iloc[:, 0]
    values = raw.to_numpy()
    return pd.Series(values, index=frame.index, dtype=float)


class TemporalCausalModel:
    """Discovers lagged correlations and builds temporal-order feature frames.

    The model tests every (cause, effect) pair over lags in
    ``[min_lag, max_lag]`` and keeps the lag with the strongest absolute
    correlation when ``|corr| >= correlation_threshold`` and
    ``p_value <= p_value_threshold``.  The retained edges then drive
    ``build_lagged_frame()`` / ``align_frame()`` which shift cause columns
    forward so that in the resulting frame a cause precedes its effect —
    exactly the ordering PyRCA's PC algorithm needs to orient edges.
    """

    def __init__(
        self,
        min_lag: int = 1,
        max_lag: int = 15,
        correlation_threshold: float = 0.5,
        p_value_threshold: float = 0.05,
    ) -> None:
        if min_lag < 1:
            raise StorageError(f"min_lag must be >= 1, got {min_lag}")
        if max_lag < min_lag:
            raise StorageError(f"max_lag ({max_lag}) must be >= min_lag ({min_lag})")
        self.min_lag = min_lag
        self.max_lag = max_lag
        self.correlation_threshold = correlation_threshold
        self.p_value_threshold = p_value_threshold

    # ------------------------------------------------------------------ #
    # Configuration
    # ------------------------------------------------------------------ #
    @classmethod
    def from_config(cls, rules_path: Path | None = None) -> TemporalCausalModel:
        """Build the model from causal_rules.yaml ``temporal_causal_model`` (best effort)."""
        path = Path(rules_path) if rules_path else _DEFAULT_RULES_PATH
        section: dict[str, Any] = {}
        try:
            with path.open("r", encoding="utf-8") as fh:
                rules: dict[str, Any] = yaml.safe_load(fh) or {}
            section = rules.get("temporal_causal_model", {}) or {}
        except OSError as exc:
            _LOG.warning("causal_rules.yaml not found; using default temporal settings: %s", exc)

        def _num(key: str, default: float) -> float:
            value = section.get(key, default)
            return default if value is None else value

        return cls(
            min_lag=int(_num("min_lag", 1)),
            max_lag=int(_num("max_lag", 15)),
            correlation_threshold=float(_num("correlation_threshold", 0.5)),
            p_value_threshold=float(_num("p_value_threshold", 0.05)),
        )

    # ------------------------------------------------------------------ #
    # Lagged correlation
    # ------------------------------------------------------------------ #
    def find_lagged_edges(
        self,
        frame: pd.DataFrame,
        effect: str,
        candidates: Sequence[str] | None = None,
    ) -> list[LaggedEdge]:
        """Return threshold-passing (cause, effect) edges with best lag.

        For every candidate cause the model scans lags ``min_lag..max_lag``
        (clamped so a shifted series keeps at least 3 overlapping points),
        picks the lag with the strongest absolute correlation, and keeps the
        pair only if ``|corr| >= correlation_threshold`` and the Fisher-z
        p-value ``<= p_value_threshold``.  Results are sorted by
        ``|correlation|`` descending (ties: cause then lag ascending).
        """
        if frame is None or frame.empty:
            return []
        if effect not in frame.columns:
            _LOG.warning("effect column %r not present in frame", effect)
            return []
        if len(frame) < 3:
            _LOG.warning("frame too short (%d rows) for lagged correlation", len(frame))
            return []

        cols = list(frame.columns)
        if candidates is None:
            candidate_cols = [c for c in cols if c != effect]
        else:
            unknown = [c for c in candidates if c not in frame.columns]
            if unknown:
                _LOG.warning("candidate columns missing from frame, dropped: %s", unknown)
            candidate_cols = [c for c in candidates if c in frame.columns and c != effect]

        max_lag = min(self.max_lag, max(1, len(frame) - 3))
        if max_lag < self.min_lag:
            _LOG.warning("max_lag %d below min_lag %d after clamping; no lags tested", max_lag, self.min_lag)
            return []

        effect_series = _column(frame, effect)
        edges: list[LaggedEdge] = []
        for cause in candidate_cols:
            cause_series = _column(frame, cause)
            if cause_series.nunique(dropna=True) < 2:
                continue
            best: tuple[float, int, float] | None = None  # (|corr|, lag, corr)
            for lag in range(self.min_lag, max_lag + 1):
                corr = cause_series.shift(lag).corr(effect_series)
                if corr is None or not math.isfinite(corr):
                    continue
                if best is None or abs(corr) > best[0]:
                    best = (abs(corr), lag, float(corr))
            if best is None:
                continue
            _, lag, corr = best
            p_value = self._p_value(corr, len(frame) - lag)
            if abs(corr) >= self.correlation_threshold and p_value <= self.p_value_threshold:
                _LOG.debug(
                    "lagged correlation: %s -> %s lag=%d corr=%.4f p=%.4f",
                    cause, effect, lag, corr, p_value,
                )
                edges.append(LaggedEdge(cause, effect, lag, corr, p_value))

        edges.sort(key=lambda e: (-abs(e.correlation), e.cause, e.lag))
        _LOG.info(
            "temporal edges: %d candidate causal edges for effect=%s",
            len(edges), effect,
        )
        return edges

    # ------------------------------------------------------------------ #
    # Lag-aligned feature frames (PC input)
    # ------------------------------------------------------------------ #
    def build_lagged_frame(
        self,
        frame: pd.DataFrame,
        effect: str,
        candidates: Sequence[str] | None = None,
        max_causes: int = 20,
    ) -> tuple[pd.DataFrame, list[LaggedEdge]]:
        """Build an effect-specific lag-aligned frame for PC.

        Cause columns are shifted forward by their best lag (so the cause
        temporally precedes ``effect``), ``effect`` stays unshifted, and all
        columns are aligned on the common non-NaN index.  Column names are
        preserved (no ``@lag`` suffixes) so root-cause names returned by
        PyRCA still match entity ids.  Returns the aligned frame plus the
        edges that produced it.
        """
        edges = self.find_lagged_edges(frame, effect, candidates)
        if not edges:
            return frame.copy(), edges
        kept = edges[: max(1, max_causes)]

        aligned: dict[str, pd.Series] = {}
        for edge in kept:
            aligned[edge.cause] = _column(frame, edge.cause).shift(edge.lag)
        aligned[effect] = _column(frame, effect)

        out = pd.DataFrame(aligned).dropna(how="any")
        if len(out) < 3:
            _LOG.warning(
                "lagged frame for effect=%s collapsed to %d rows; falling back to raw frame",
                effect, len(out),
            )
            return frame.copy(), kept
        return out, kept

    def align_frame(
        self,
        frame: pd.DataFrame,
        edges: Sequence[LaggedEdge] | None = None,
    ) -> pd.DataFrame:
        """Globally align a frame from a set of edges.

        Each cause column is shifted by the *smallest* lag among the edges it
        participates in (a cause cannot be shifted twice in one wide frame), so
        the output keeps one shifted column per entity.  Columns with no edge
        stay unshifted.  Rows containing any NaN are dropped.
        """
        if frame is None or frame.empty:
            return frame
        if not edges:
            return frame

        lag_by_cause: dict[str, int] = {}
        for edge in edges:
            if edge.cause not in lag_by_cause or edge.lag < lag_by_cause[edge.cause]:
                lag_by_cause[edge.cause] = edge.lag

        aligned: dict[str, pd.Series] = {}
        for col in frame.columns:
            lag = lag_by_cause.get(col, 0)
            series = _column(frame, col)
            aligned[col] = series.shift(lag) if lag else series
        out = pd.DataFrame(aligned).dropna(how="any")
        if len(out) < 3:
            _LOG.warning(
                "aligned frame collapsed to %d rows after shifts; returning raw frame",
                len(out),
            )
            return frame.copy()
        return out

    # ------------------------------------------------------------------ #
    # Significance helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _p_value(corr: float, n: int) -> float:
        """Two-sided p-value for Pearson correlation via Fisher-z.

        Uses the standard normal approximation ``z = atanh(r) * sqrt(n - 3)``
        which is accurate for n >= 10 and adequate for thresholding at 0.05.
        Pure stdlib (math.erf) so the host smoke tests never need scipy.
        """
        if n < 4 or not math.isfinite(corr):
            return 1.0
        z = math.atanh(max(-0.999999, min(0.999999, corr))) * math.sqrt(max(1.0, n - 3))
        return 2.0 * (1.0 - _normal_cdf(abs(z)))


def _normal_cdf(x: float) -> float:
    """Standard normal CDF from math.erf (pure stdlib)."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))
