"""
OmniWatch — User Service
Component: API Routes
Phase: 1
Purpose: FastAPI CRUD endpoints for user management with OTel metrics and anomaly injection
Inputs: HTTP requests to /api/v1/users/*
Outputs: JSON responses with User models
"""

import asyncio
import logging
import random
import time
from typing import Any

from crud import create_user, delete_user, get_user, list_users, update_user
from fastapi import APIRouter, HTTPException, Request
from models import User, UserCreate, UserUpdate
from opentelemetry import metrics

logger = logging.getLogger("omniwatch.user_service.routes")

# ---------------------------------------------------------------------------
# OTel instruments
# ---------------------------------------------------------------------------
_meter: metrics.Meter = metrics.get_meter("user-service")
_request_counter = _meter.create_counter(
    name="user_service.requests.total",
    description="Total number of user-service API requests",
    unit="1",
)
_latency_histogram = _meter.create_histogram(
    name="user_service.request.duration",
    description="Request latency in seconds",
    unit="s",
)

router = APIRouter(prefix="/api/v1/users")


# ---------------------------------------------------------------------------
# Anomaly helpers
# ---------------------------------------------------------------------------


async def _apply_anomaly_delay(request: Request) -> None:
    """Apply artificial delay if latency_spike anomaly is active.

    Reads the delay_ms from the engine's scenario payload and sleeps.
    """
    engine = request.app.state.engine
    if engine.is_active("latency_spike"):
        payload: dict[str, Any] = engine.apply("latency_spike")
        delay_ms = payload.get("delay_ms", 0)
        if delay_ms > 0:
            logger.warning("anomaly=latency_spike delay_ms=%d", delay_ms)
            await asyncio.sleep(delay_ms / 1000.0)


async def _maybe_raise_error(request: Request) -> None:
    """Randomly raise a 503 error if database_cascade anomaly is active.

    Uses the error_rate from the engine's scenario payload to decide
    whether to simulate a downstream failure.
    """
    engine = request.app.state.engine
    if engine.is_active("database_cascade"):
        payload: dict[str, Any] = engine.apply("database_cascade")
        error_rate = payload.get("error_rate", 0.0)
        if error_rate > 0 and random.random() < error_rate:
            logger.warning("anomaly=database_cascade error_rate=%.2f", error_rate)
            raise HTTPException(
                status_code=503,
                detail="Simulated downstream failure (database_cascade)",
            )


async def _record_metrics(method: str, path: str, start: float, status: int) -> None:
    """Record request count and latency to OTel metrics."""
    elapsed = time.time() - start
    _request_counter.add(
        1,
        {"http.method": method, "http.route": path, "http.status_code": str(status)},
    )
    _latency_histogram.record(
        elapsed,
        {"http.method": method, "http.route": path, "http.status_code": str(status)},
    )


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------


@router.post("/", response_model=User, status_code=201, summary="Create a new user")
async def create_user_route(request: Request, body: UserCreate) -> User:
    """Create a user with the provided name and email."""
    start = time.time()
    try:
        await _apply_anomaly_delay(request)
        await _maybe_raise_error(request)
        user = create_user(body)
        logger.info("user_created id=%s name=%s", user.id, user.name)
        return user
    finally:
        await _record_metrics("POST", "/api/v1/users", start, 201)


@router.get("/", response_model=list[User], summary="List all users")
async def list_users_route(request: Request) -> list[User]:
    """Return all stored users."""
    start = time.time()
    try:
        await _apply_anomaly_delay(request)
        await _maybe_raise_error(request)
        users = list_users()
        logger.info("users_listed count=%d", len(users))
        return users
    finally:
        await _record_metrics("GET", "/api/v1/users", start, 200)


@router.get("/{user_id}", response_model=User, summary="Get user by ID")
async def get_user_route(request: Request, user_id: str) -> User:
    """Retrieve a single user by their UUID."""
    start = time.time()
    try:
        await _apply_anomaly_delay(request)
        await _maybe_raise_error(request)
        user = get_user(user_id)
        if user is None:
            raise HTTPException(status_code=404, detail=f"User {user_id} not found")
        logger.info("user_fetched id=%s", user_id)
        return user
    finally:
        await _record_metrics("GET", f"/api/v1/users/{user_id}", start, 200)


@router.put("/{user_id}", response_model=User, summary="Update user by ID")
async def update_user_route(request: Request, user_id: str, body: UserUpdate) -> User:
    """Update a user's name and/or email by their UUID."""
    start = time.time()
    try:
        await _apply_anomaly_delay(request)
        await _maybe_raise_error(request)
        user = update_user(user_id, body)
        if user is None:
            raise HTTPException(status_code=404, detail="user not found")
        logger.info("user_updated id=%s", user_id)
        return user
    finally:
        await _record_metrics("PUT", f"/api/v1/users/{user_id}", start, 200)


@router.delete("/{user_id}", status_code=204, summary="Delete user by ID")
async def delete_user_route(request: Request, user_id: str) -> None:
    """Delete a user by their UUID."""
    start = time.time()
    try:
        await _apply_anomaly_delay(request)
        await _maybe_raise_error(request)
        deleted = delete_user(user_id)
        if not deleted:
            raise HTTPException(status_code=404, detail=f"User {user_id} not found")
        logger.info("user_deleted id=%s", user_id)
    finally:
        await _record_metrics("DELETE", f"/api/v1/users/{user_id}", start, 204)
