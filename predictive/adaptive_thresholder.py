"""
OmniWatch — Predictive Intelligence Layer
Component: Adaptive Thresholder
Phase: 6
Purpose: Welford's online baseline computation for adaptive thresholds
Inputs: Metric values per entity
Outputs: Adaptive threshold per entity/metric
"""

from __future__ import annotations

import json
import math
import os
import threading
from pathlib import Path
from typing import Dict, Optional


class _WelfordStats:
    """Single (entity_id, metric) running statistics via Welford's online algorithm."""

    __slots__ = ("count", "mean", "m2")

    def __init__(self, count: int = 0, mean: float = 0.0, m2: float = 0.0) -> None:
        self.count = count
        self.mean = mean
        self.m2 = m2  # sum of squared deviations from current mean

    def update(self, value: float) -> None:
        """O(1) incremental update using Welford's algorithm."""
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        delta2 = value - self.mean
        self.m2 += delta * delta2

    @property
    def variance(self) -> float:
        """Population variance (0 when count < 2)."""
        return self.m2 / self.count if self.count >= 2 else 0.0

    @property
    def stddev(self) -> float:
        return math.sqrt(self.variance)

    def to_dict(self) -> Dict[str, float]:
        return {"count": self.count, "mean": self.mean, "m2": self.m2}

    @classmethod
    def from_dict(cls, d: Dict[str, float]) -> "_WelfordStats":
        return cls(count=int(d["count"]), mean=d["mean"], m2=d["m2"])


class AdaptiveThresholder:
    """Maintains per-entity/metric adaptive thresholds using Welford's online algorithm.

    Thread-safe.  State is persisted atomically (tmp + os.replace) so a crash
    mid-write never corrupts the on-disk file.
    """

    def __init__(self, state_path: Optional[str] = None, k: float = 3.0) -> None:
        """
        Args:
            state_path: Path to JSON state file.  None = in-memory only.
            k: Number of standard deviations above the mean for the threshold.
        """
        self._k = k
        self._state_path = Path(state_path) if state_path else None
        self._lock = threading.Lock()
        self._stats: Dict[str, _WelfordStats] = {}  # key = "entity_id::metric"

        if self._state_path and self._state_path.exists():
            self._load()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def update(self, entity_id: str, metric: str, value: float) -> None:
        """Record a new observation.  O(1)."""
        key = f"{entity_id}::{metric}"
        with self._lock:
            stats = self._stats.get(key)
            if stats is None:
                stats = _WelfordStats()
                self._stats[key] = stats
            stats.update(value)
            if self._state_path:
                self._save()

    def get_threshold(self, entity_id: str, metric: str) -> Optional[float]:
        """Return *mean + k * stddev* for the key, or None if no data yet."""
        key = f"{entity_id}::{metric}"
        with self._lock:
            stats = self._stats.get(key)
            if stats is None or stats.count < 2:
                return None
            return stats.mean + self._k * stats.stddev

    def get_stats(self, entity_id: str, metric: str) -> Optional[Dict[str, float]]:
        """Return raw Welford stats dict (useful for debugging / tests)."""
        key = f"{entity_id}::{metric}"
        with self._lock:
            stats = self._stats.get(key)
            if stats is None:
                return None
            return {
                "count": stats.count,
                "mean": stats.mean,
                "variance": stats.variance,
                "stddev": stats.stddev,
                "m2": stats.m2,
            }

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _save(self) -> None:
        """Atomic JSON write: tmp + os.replace."""
        assert self._state_path is not None
        data = {
            "k": self._k,
            "stats": {
                key: s.to_dict() for key, s in self._stats.items()
            },
        }
        tmp_path = self._state_path.with_suffix(".tmp")
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)
        os.replace(str(tmp_path), str(self._state_path))

    def _load(self) -> None:
        """Load state from disk.  Called once in __init__."""
        assert self._state_path is not None
        with open(self._state_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        self._k = data.get("k", self._k)
        for key, sdict in data.get("stats", {}).items():
            self._stats[key] = _WelfordStats.from_dict(sdict)
