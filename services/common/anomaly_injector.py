"""
OmniWatch — Shared Anomaly Injection Module
Component: AnomalyEngine
Phase: 1 (Simulation Layer)
Purpose: Provides reusable anomaly injection for all services — 5 scenarios with
         auto-revert TTL, thread-safe state, and FastAPI route integration.
Inputs: HTTP requests via /__inject/anomaly endpoints, scenario names + TTL
Outputs: Observable service behavior modifications (delay, errors, memory, logs)
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger("omniwatch.anomaly_injector")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALID_SCENARIOS: list[str] = [
    "database_cascade",
    "memory_leak",
    "latency_spike",
    "security_attack",
    "config_drift",
]


# ---------------------------------------------------------------------------
# Request / Response models (Pydantic v2)
# ---------------------------------------------------------------------------


class InjectRequest(BaseModel):
    """Body for POST /__inject/anomaly."""

    scenario: str = Field(..., description="Anomaly scenario to activate")
    ttl_seconds: int = Field(
        default=60, ge=1, le=3600, description="Time-to-live in seconds"
    )


class ActiveAnomaly(BaseModel):
    """Single active anomaly entry returned by GET /__inject/anomaly."""

    scenario: str
    remaining_seconds: float
    expires_at: float


class ActiveAnomaliesResponse(BaseModel):
    """Response for GET /__inject/anomaly."""

    service: str
    active: list[ActiveAnomaly]


class InjectResponse(BaseModel):
    """Response for POST /__inject/anomaly."""

    status: str
    scenario: str
    ttl_seconds: int
    expires_at: float


class ClearResponse(BaseModel):
    """Response for DELETE operations."""

    status: str
    cleared: str


# ---------------------------------------------------------------------------
# Scenario definitions — each returns its "apply" payload
# ---------------------------------------------------------------------------

_SCENARIO_PAYLOADS: dict[str, dict[str, Any]] = {
    "database_cascade": {"delay_ms": 2000, "error_rate": 0.3},
    "memory_leak": {"extra_memory_mb": 50, "response_bloat": True},
    "latency_spike": {"delay_ms": 3000},
    "security_attack": {"block_ip": True, "log_frequency": "high"},
    "config_drift": {
        "config_version": "drifted",
        "features_disabled": ["cache", "rate_limit"],
    },
}


# ---------------------------------------------------------------------------
# AnomalyEngine
# ---------------------------------------------------------------------------


class AnomalyEngine:
    """Thread-safe anomaly injection engine for a single service.

    Usage::

        engine = AnomalyEngine(service_name="order-service")

        # Inject
        engine.inject("database_cascade", ttl_seconds=120)

        # Check from request handler
        if engine.is_active("database_cascade"):
            payload = engine.apply("database_cascade", service_context={})
            # apply artificial delay, raise errors, etc.

        # Clean up
        engine.clear("database_cascade")
    """

    def __init__(self, service_name: str) -> None:
        self.service_name = service_name
        # scenario_name -> expiration timestamp (time.time() based)
        self._active: dict[str, float] = {}
        self._lock = threading.Lock()

    # -- Internal helpers ----------------------------------------------------

    def _evict_expired(self) -> None:
        """Remove expired entries. Caller MUST hold self._lock."""
        now = time.time()
        expired = [s for s, exp in self._active.items() if exp <= now]
        for s in expired:
            del self._active[s]
            logger.info(
                "[anomaly_injector] scenario auto-reverted: service=%s scenario=%s",
                self.service_name,
                s,
            )

    # -- Public API ----------------------------------------------------------

    def inject(self, scenario: str, ttl_seconds: int = 60) -> dict[str, Any]:
        """Activate an anomaly scenario with a TTL.

        Returns a summary dict with status, scenario, ttl, and expires_at.
        Raises ValueError for unknown scenarios.
        """
        if scenario not in VALID_SCENARIOS:
            raise ValueError(f"Unknown scenario '{scenario}'. Valid: {VALID_SCENARIOS}")

        expires_at = time.time() + ttl_seconds
        with self._lock:
            self._active[scenario] = expires_at

        logger.info(
            "[anomaly_injector] injected: service=%s scenario=%s ttl=%ds expires_at=%.1f",
            self.service_name,
            scenario,
            ttl_seconds,
            expires_at,
        )
        return {
            "status": "injected",
            "scenario": scenario,
            "ttl_seconds": ttl_seconds,
            "expires_at": expires_at,
        }

    def clear(self, scenario: str) -> None:
        """Deactivate a specific scenario (no-op if not active)."""
        with self._lock:
            self._active.pop(scenario, None)
        logger.info(
            "[anomaly_injector] cleared: service=%s scenario=%s",
            self.service_name,
            scenario,
        )

    def clear_all(self) -> None:
        """Deactivate all scenarios."""
        with self._lock:
            self._active.clear()
        logger.info(
            "[anomaly_injector] cleared_all: service=%s",
            self.service_name,
        )

    def is_active(self, scenario: str) -> bool:
        """Check if a scenario is currently active (auto-evicts expired)."""
        with self._lock:
            self._evict_expired()
            if scenario not in self._active:
                return False
            return self._active[scenario] > time.time()

    def get_active(self) -> list[dict[str, Any]]:
        """Return list of active anomalies with remaining TTL."""
        with self._lock:
            self._evict_expired()
            now = time.time()
            result = []
            for scenario, expires_at in self._active.items():
                remaining = max(0.0, expires_at - now)
                result.append(
                    {
                        "scenario": scenario,
                        "remaining_seconds": round(remaining, 2),
                        "expires_at": expires_at,
                    }
                )
            return result

    def apply(
        self, scenario: str, service_context: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Return the observable payload for an active scenario.

        If the scenario is not active, returns an empty dict (no effect).
        The ``service_context`` parameter is reserved for future per-service
        customization and is currently unused.
        """
        if not self.is_active(scenario):
            return {}
        return dict(_SCENARIO_PAYLOADS.get(scenario, {}))


# Backwards-compatible alias used by plan QA scripts and the simulation layer.
AnomalyInjector = AnomalyEngine


# ---------------------------------------------------------------------------
# FastAPI router integration
# ---------------------------------------------------------------------------


def add_routes(router: APIRouter, engine: AnomalyEngine) -> None:
    """Register ``/__inject/anomaly`` endpoints on the given router.

    Example::

        from fastapi import FastAPI
        from services.common.anomaly_injector import AnomalyEngine, add_routes

        app = FastAPI()
        engine = AnomalyEngine(service_name="my-service")
        add_routes(app.router, engine)
    """

    @router.post(
        "/__inject/anomaly",
        response_model=InjectResponse,
        tags=["anomaly-injection"],
        summary="Inject an anomaly scenario",
    )
    def inject_anomaly(body: InjectRequest) -> InjectResponse:
        try:
            result = engine.inject(body.scenario, body.ttl_seconds)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return InjectResponse(**result)

    @router.delete(
        "/__inject/anomaly/{scenario}",
        response_model=ClearResponse,
        tags=["anomaly-injection"],
        summary="Clear a specific anomaly scenario",
    )
    def clear_scenario(scenario: str) -> ClearResponse:
        if scenario not in VALID_SCENARIOS:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown scenario '{scenario}'. Valid: {VALID_SCENARIOS}",
            )
        engine.clear(scenario)
        return ClearResponse(status="cleared", cleared=scenario)

    @router.delete(
        "/__inject/anomaly",
        response_model=ClearResponse,
        tags=["anomaly-injection"],
        summary="Clear all active anomaly scenarios",
    )
    def clear_all_scenarios() -> ClearResponse:
        engine.clear_all()
        return ClearResponse(status="cleared", cleared="all")

    @router.get(
        "/__inject/anomaly",
        response_model=ActiveAnomaliesResponse,
        tags=["anomaly-injection"],
        summary="List active anomaly scenarios",
    )
    def list_active() -> ActiveAnomaliesResponse:
        active = engine.get_active()
        return ActiveAnomaliesResponse(
            service=engine.service_name,
            active=[ActiveAnomaly(**a) for a in active],
        )
