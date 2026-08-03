"""
OmniWatch — Causal Graph Engine
Component: PyRCA Adapter
Phase: 7
Purpose: Wrap PyRCA PC causal discovery + RandomWalk root-cause scoring behind a
         stable interface for the Layer-2 causal graph.
Inputs: time-series DataFrame (index=timestamp, columns=metrics), anomalous metric
        names, causal_rules.yaml (pc_algorithm / random_walk sections)
Outputs: causal adjacency matrix (row=cause -> col=effect), root-cause entities
         with scores, fault paths root -> ... -> symptom
"""
from __future__ import annotations

import logging
import types
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from storage.common import StorageError, create_logger

_LOG: logging.Logger = create_logger("omniwatch.causal.py_rca_adapter")

_DEFAULT_RULES_PATH: Path = Path(__file__).resolve().parent / "config" / "causal_rules.yaml"

_ROOT_PREFIX = "ROOT_"


def _strip_root(name: str) -> str:
    """PyRCA can prefix root nodes with ``ROOT_``; remove it when present."""
    if isinstance(name, str) and name.startswith(_ROOT_PREFIX):
        return name[len(_ROOT_PREFIX):]
    return name


class PyRCAAdapter:
    """PyRCA PC + RandomWalk adapter.

    The causal adjacency matrix follows PyRCA convention:
    ``adjacency_df.at[cause, effect] > 0`` means an edge ``cause -> effect``
    (fault flows cause -> effect), which matches the Layer-2 orientation used by
    :mod:`causal.two_layer_graph`.
    """

    def __init__(
        self,
        *,
        alpha: float = 0.05,
        run_pdag2dag: bool = True,
        max_num_points: int | None = 100000,
        domain_knowledge_file: str | None = None,
        use_bayesian: bool = False,
        rho: float = 0.1,
        num_steps: int = 10,
        num_repeats: int = 1000,
        root_cause_top_k: int = 5,
        random_seed: int = 42,
    ) -> None:
        self.alpha = alpha
        self.run_pdag2dag = run_pdag2dag
        self.max_num_points = max_num_points
        self.domain_knowledge_file = domain_knowledge_file
        self.use_bayesian = use_bayesian
        self.rho = rho
        self.num_steps = num_steps
        self.num_repeats = num_repeats
        self.root_cause_top_k = root_cause_top_k
        self.random_seed = random_seed
        self.last_graph: pd.DataFrame | None = None

    # ------------------------------------------------------------------ config
    @classmethod
    def from_config(cls, rules_path: Path | None = None) -> PyRCAAdapter:
        """Build an adapter from causal_rules.yaml (best effort)."""
        path = Path(rules_path) if rules_path else _DEFAULT_RULES_PATH
        try:
            with path.open("r", encoding="utf-8") as fh:
                import yaml

                rules: dict[str, Any] = yaml.safe_load(fh) or {}
        except OSError as exc:
            _LOG.warning("causal_rules.yaml not found; using default PyRCA settings: %s", exc)
            rules = {}

        pc: dict[str, Any] = rules.get("pc_algorithm", {}) or {}
        rw: dict[str, Any] = rules.get("random_walk", {}) or {}

        def _num(section: dict[str, Any], key: str, default: float) -> float:
            value = section.get(key, default)
            return default if value is None else value

        return cls(
            alpha=float(_num(pc, "alpha", 0.05)),
            run_pdag2dag=bool(pc.get("run_pdag2dag", True)),
            max_num_points=int(pc.get("max_num_points", 100000)) if pc.get("max_num_points") else None,
            domain_knowledge_file=pc.get("domain_knowledge_file"),
            use_bayesian=bool(pc.get("use_bayesian", False)),
            rho=float(_num(rw, "rho", 0.1)),
            num_steps=int(_num(rw, "num_steps", 10)),
            num_repeats=int(_num(rw, "num_repeats", 1000)),
            root_cause_top_k=int(_num(rw, "root_cause_top_k", 5)),
            random_seed=int(_num(rw, "random_seed", 42)),
        )

    # --------------------------------------------------------------- training
    @staticmethod
    def _require_pyrca() -> tuple[Any, Any, Any, Any]:
        """Lazily import PyRCA so static analysis / non-PyRCA hosts stay clean."""
        try:
            from pyrca.analyzers.random_walk import RandomWalk, RandomWalkConfig
            from pyrca.graphs.causal.pc import PC, PCConfig
        except ImportError as exc:  # pragma: no cover - exercised in docker only
            raise StorageError(
                "PyRCA (sfr-pyrca) is required for causal discovery; "
                "install via causal/requirements.txt"
            ) from exc
        return PC, PCConfig, RandomWalk, RandomWalkConfig

    @staticmethod
    def _transpose_guard(df: pd.DataFrame) -> pd.DataFrame:
        """PyRCA expects index=timestamp, columns=metrics — never transpose here."""
        if df.shape[1] > df.shape[0]:
            _LOG.warning(
                "possible transposed frame: %d columns > %d rows; expected "
                "index=timestamp, columns=metrics (not transposing)",
                df.shape[1],
                df.shape[0],
            )
        return df

    def discover_graph(self, df: pd.DataFrame) -> pd.DataFrame:
        """Learn the causal adjacency matrix via the PyRCA PC algorithm.

        :param df: historical time series, index=timestamp, columns=metrics.
        :returns: adjacency DataFrame where ``at[cause, effect] > 0`` => cause->effect.
        """
        if not isinstance(df, pd.DataFrame) or df.empty:
            raise StorageError("discover_graph requires a non-empty DataFrame")

        df = self._transpose_guard(df)
        if self.max_num_points is not None and len(df) > self.max_num_points:
            df = df.iloc[: self.max_num_points]

        PC, PCConfig, _, _ = self._require_pyrca()
        config = PCConfig(
            alpha=self.alpha,
            run_pdag2dag=self.run_pdag2dag,
            max_num_points=self.max_num_points,
            domain_knowledge_file=self.domain_knowledge_file,
        )
        try:
            adjacency_df: pd.DataFrame = PC(config=config).train(df)
        except RuntimeError as exc:
            raise StorageError(f"PyRCA PC failed to orient the causal DAG: {exc}") from exc

        self.last_graph = adjacency_df.astype(float).copy()
        _LOG.info(
            "PC causal discovery: %d nodes, %d directed edges",
            self.last_graph.shape[0],
            int((self.last_graph.values > 0).sum()),
        )
        return self.last_graph

    def train(self, df: pd.DataFrame) -> pd.DataFrame:
        """Alias for :meth:`discover_graph` (train/score naming symmetry)."""
        return self.discover_graph(df)

    @staticmethod
    def to_edges(adjacency_df: pd.DataFrame) -> list[tuple[str, str, float]]:
        """Flatten an adjacency matrix into ``(cause, effect, weight)`` triples."""
        edges: list[tuple[str, str, float]] = []
        for cause in adjacency_df.index:
            for effect in adjacency_df.columns:
                weight = adjacency_df.at[cause, effect]
                if weight and weight > 0:
                    edges.append((str(cause), str(effect), float(weight)))
        return edges

    # ---------------------------------------------------------- root cause
    def find_root_causes(
        self,
        anomalous_metrics: Sequence[str],
        df: pd.DataFrame,
        random_seed: int | None = None,
    ) -> tuple[list[str], list[float], dict[str, list[list[str]]]]:
        """Score root causes on the learned causal graph.

        :param anomalous_metrics: metric names flagged as anomalous in the window.
        :param df: incident-window time series (index=timestamp, columns=metrics).
        :returns: ``(root_entities, scores, fault_paths)`` where ``fault_paths``
            maps a root entity to ordered paths [root -> ... -> symptom].
        """
        if not isinstance(df, pd.DataFrame) or df.empty:
            raise StorageError("find_root_causes requires a non-empty DataFrame")

        df = self._transpose_guard(df)
        if self.last_graph is None:
            self.discover_graph(df)

        anomalies = [str(m) for m in anomalous_metrics]
        missing = [m for m in anomalies if m not in set(df.columns)]
        if missing:
            _LOG.warning("anomalous metrics missing from frame, dropped: %s", missing)
        anomalies = [m for m in anomalies if m in set(df.columns)]
        if not anomalies:
            raise StorageError("no anomalous metrics present in the DataFrame columns")

        seed = self.random_seed if random_seed is None else random_seed

        if self.use_bayesian:
            results = self._score_bayesian(anomalies, df, seed)
        else:
            results = self._random_walk_scores(anomalies, df, seed)

        root_cause_nodes = list(getattr(results, "root_cause_nodes", []) or [])
        root_cause_paths = dict(getattr(results, "root_cause_paths", {}) or {})

        root_entities = [_strip_root(str(name)) for name, _ in root_cause_nodes]
        scores = [float(score) for _, score in root_cause_nodes]

        fault_paths: dict[str, list[list[str]]] = {}
        for root, paths in root_cause_paths.items():
            ordered = sorted(paths, key=lambda item: float(item[0]), reverse=True)
            fault_paths[_strip_root(str(root))] = [
                [_strip_root(str(node)) for node, _ in path_nodes] for _, path_nodes in ordered
            ]

        _LOG.info(
            "root cause analysis: scorer=%s candidates=%s top=%s",
            "bayesian" if self.use_bayesian else "random_walk",
            root_entities,
            scores,
        )
        return root_entities, scores, fault_paths

    def _random_walk_scores(self, anomalies: list[str], df: pd.DataFrame, seed: int) -> Any:
        """Score root causes via PyRCA RandomWalk, with a correlation fallback.

        PyRCA's RandomWalk builds per-node walk probabilities as ``w / sum(w)``;
        when the anomalous node is a graph root (no incoming edges) every weight
        is zero and the probabilities become NaN, crashing the scorer. In that
        case we fall back to ranking candidates by absolute correlation with the
        anomalous window, which always yields a deterministic answer.
        """
        _, _, RandomWalk, RandomWalkConfig = self._require_pyrca()
        walk = RandomWalk(
            config=RandomWalkConfig(
                graph=self.last_graph,
                rho=self.rho,
                num_steps=self.num_steps,
                num_repeats=self.num_repeats,
                root_cause_top_k=self.root_cause_top_k,
            )
        )
        try:
            return walk.find_root_causes(anomalous_metrics=anomalies, df=df, random_seed=seed)
        except Exception as exc:  # noqa: BLE001 - NaN probabilities on root anomalies
            _LOG.warning("random walk scorer failed (%s); using correlation fallback", exc)
            return self._correlation_fallback(anomalies, df)

    def _correlation_fallback(self, anomalies: list[str], df: pd.DataFrame) -> Any:
        """Deterministic correlation-based root-cause ranking.

        Builds the same result shape PyRCA returns (``root_cause_nodes`` as
        ``(name, score)`` pairs and ``root_cause_paths`` keyed by root) so the
        public :meth:`find_root_causes` contract is unchanged.
        """
        assert self.last_graph is not None, "discover_graph() must run first"

        scores: dict[str, float] = {}
        for anomaly in anomalies:
            for metric in df.columns:
                if metric == anomaly:
                    continue
                corr = df[anomaly].corr(df[metric])
                if corr is None or np.isnan(corr):
                    continue
                scores[metric] = max(scores.get(metric, 0.0), float(abs(corr)))

        ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)[: self.root_cause_top_k]
        root_cause_nodes = [(name, score) for name, score in ranked]
        root_cause_paths = {name: [] for name, _ in ranked}

        results = types.SimpleNamespace(
            root_cause_nodes=root_cause_nodes,
            root_cause_paths=root_cause_paths,
        )
        _LOG.info("correlation fallback: candidates=%s", [name for name, _ in ranked])
        return results

    def _score_bayesian(self, anomalies: list[str], df: pd.DataFrame, seed: int) -> Any:
        """Optional BayesianNetwork scorer; falls back to RandomWalk on failure."""
        try:
            from pyrca.analyzers.bayesian import BayesianNetwork
        except ImportError as exc:
            raise StorageError("PyRCA Bayesian scorer unavailable") from exc
        try:
            model = BayesianNetwork(config=BayesianNetwork.config_class(graph=self.last_graph))
            model.train(dfs=[df])
            return model.find_root_causes(anomalous_metrics=anomalies)
        except Exception as exc:  # noqa: BLE001 - experimental scorer fallback
            _LOG.warning("bayesian scorer failed (%s); falling back to random walk", exc)
            return self._random_walk_scores(anomalies, df, seed)
