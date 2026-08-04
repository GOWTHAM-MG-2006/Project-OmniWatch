"""
OmniWatch — Predictive Intelligence Layer
Component: K8s Event Integration
Phase: 6
Purpose: Integrate Kubernetes events to adjust anomaly detection baselines
Inputs: Kubernetes events (in-cluster config or kubeconfig), K8S_NAMESPACE env var
Outputs: Baseline adjustment factor (1.0 or 1.5) for the anomaly detector
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

# 5-minute cooldown between Kubernetes API calls (avoid hammering the API).
COOLDOWN_SECONDS = 300

# A relevant event keeps the baseline adjusted for this long after it is seen.
RELEVANCE_WINDOW_SECONDS = 300

# Adjustment factor returned while a relevant event is recently observed.
RELEVANT_EVENT_ADJUSTMENT = 1.5

# Event reasons that indicate node pressure, restarts, or evictions.
RELEVANT_EVENT_REASONS = frozenset(
    {
        # Node pressure conditions surfaced as events.
        "memorypressure",
        "diskpressure",
        "pidpressure",
        # Node state changes.
        "nodenotready",
        "nodeready",
        "rebooted",
        # Container restarts / crash loops.
        "backoff",
        "crashloopbackoff",
        "killing",
        # Evictions and preemption.
        "evicted",
        "evicting",
        "preempting",
        "nodememorypressure",
        "nodediskpressure",
    }
)

# Message keywords used as a fallback when the reason is not in the known set.
RELEVANT_MESSAGE_KEYWORDS = ("pressure", "evict", "restart", "crashloop")

# Bound the number of events fetched per API call.
EVENT_LIST_LIMIT = 50


class K8sEventIntegration:
    """Integrates Kubernetes events to adjust anomaly detection baselines.

    The ``kubernetes`` client is imported lazily so this module imports and
    runs cleanly even when the package is not installed. Connection resolution
    order: (1) in-cluster config, (2) kubeconfig file, (3) neither available
    -> ``disabled`` mode (no-op, always returns 1.0).

    A 5-minute cooldown prevents hammering the Kubernetes API. When a relevant
    event (node pressure, restart, eviction) is observed within the relevance
    window, ``get_baseline_adjustment`` returns 1.5; otherwise 1.0.
    """

    def __init__(
        self,
        namespace: Optional[str] = None,
        clock: Optional[Callable[[], float]] = None,
        client: Any = None,
    ) -> None:
        """
        Args:
            namespace: Kubernetes namespace to watch. Defaults to the
                ``K8S_NAMESPACE`` env var, then ``"default"``.
            clock: Callable returning the current epoch seconds. Defaults to
                ``time.time``. Injectable for deterministic tests.
            client: Optional pre-built Kubernetes CoreV1Api client. When
                provided the integration is treated as enabled without
                attempting config resolution (used by tests / callers that
                already hold a client).
        """
        self._namespace = namespace or os.environ.get("K8S_NAMESPACE", "default")
        self._clock = clock or time.time
        self._client: Any = None
        self._enabled = False
        self._last_api_call = 0.0
        self._last_relevant_event_time = 0.0

        if client is not None:
            self._client = client
            self._enabled = True
        else:
            self._connect()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def enabled(self) -> bool:
        """True when a Kubernetes connection is available, False otherwise."""
        return self._enabled

    def get_baseline_adjustment(self) -> float:
        """Return the baseline adjustment factor for the anomaly detector.

        Returns 1.5 when a relevant Kubernetes event (node pressure, restart,
        eviction) was observed within the relevance window, else 1.0. In
        disabled mode always returns 1.0.
        """
        if not self._enabled:
            return 1.0

        now = self._clock()
        if now - self._last_api_call >= COOLDOWN_SECONDS:
            self._last_api_call = now
            self._refresh()

        if now - self._last_relevant_event_time <= RELEVANCE_WINDOW_SECONDS:
            return RELEVANT_EVENT_ADJUSTMENT
        return 1.0

    def reset(self) -> None:
        """Clear cached event state so the adjustment returns to 1.0.

        The API cooldown is preserved so a reset does not trigger an immediate
        re-poll of the Kubernetes API.
        """
        self._last_relevant_event_time = 0.0

    # ------------------------------------------------------------------
    # Connection resolution
    # ------------------------------------------------------------------

    def _connect(self) -> None:
        """Resolve a Kubernetes connection, degrading to disabled on failure.

        Order: (1) in-cluster config, (2) kubeconfig file. If neither is
        available (or the ``kubernetes`` package is absent), the integration
        is disabled and becomes a no-op.
        """
        try:
            import kubernetes  # lazy import: kubernetes is optional
        except ImportError:
            logger.warning("kubernetes package not installed; K8s integration disabled")
            self._enabled = False
            return

        try:
            kubernetes.config.load_incluster_config()
        except Exception:
            try:
                kubernetes.config.load_kube_config()
            except Exception:
                logger.warning("no in-cluster or kubeconfig found; K8s integration disabled")
                self._enabled = False
                return

        self._client = kubernetes.client.CoreV1Api()
        self._enabled = True

    # ------------------------------------------------------------------
    # Event fetching / relevance
    # ------------------------------------------------------------------

    def _refresh(self) -> None:
        """Fetch recent events and record whether a relevant one was seen."""
        try:
            events = self._client.list_namespaced_event(
                namespace=self._namespace, limit=EVENT_LIST_LIMIT
            )
        except Exception:
            # API failure: keep previous state, never crash the caller.
            logger.warning("failed to list Kubernetes events; keeping prior state")
            return

        items = getattr(events, "items", events) or []
        if any(self._event_is_relevant(ev) for ev in items):
            self._last_relevant_event_time = self._clock()

    def _event_is_relevant(self, event: Any) -> bool:
        """Return True when an event signals node pressure, restart, or eviction."""
        reason = (getattr(event, "reason", "") or "").lower()
        message = (getattr(event, "message", "") or "").lower()

        if reason in RELEVANT_EVENT_REASONS:
            return True
        return any(keyword in message for keyword in RELEVANT_MESSAGE_KEYWORDS)