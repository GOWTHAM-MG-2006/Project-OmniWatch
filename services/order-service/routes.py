"""
OmniWatch — Order Service
Component: HTTP Routes
Phase: 1
Purpose: FastAPI router for order CRUD endpoints with OTel metrics and anomaly injection
Inputs: HTTP requests to /api/v1/orders/*
Outputs: JSON responses with Order data
"""

import logging
import random
import time

from fastapi import APIRouter, Depends, HTTPException, Request

from services.common.anomaly_injector import AnomalyEngine
from services.common.otel_setup import get_meter
from models import Order, OrderCreate
from crud import get_order, list_orders, list_orders_by_user
from saga import create_order_saga

logger = logging.getLogger("omniwatch.order_service.routes")

router = APIRouter(prefix="/api/v1/orders")


# ---------------------------------------------------------------------------
# Dependency — extract AnomalyEngine from app state
# ---------------------------------------------------------------------------


def _get_engine(request: Request) -> AnomalyEngine:
    """FastAPI dependency that returns the service's AnomalyEngine.

    The engine is attached to ``app.state.anomaly_engine`` by ``main.py``
    during startup.
    """
    engine: AnomalyEngine = request.app.state.anomaly_engine
    return engine


# ---------------------------------------------------------------------------
# Anomaly check helper
# ---------------------------------------------------------------------------


def _check_anomaly(engine: AnomalyEngine) -> None:
    """Apply active anomaly effects (delay, error injection) in-place.

    Raises HTTPException for anomaly-driven errors.
    """
    scenarios_to_check = [
        "database_cascade",
        "memory_leak",
        "latency_spike",
        "security_attack",
        "config_drift",
    ]

    for scenario in scenarios_to_check:
        if engine.is_active(scenario):
            payload = engine.apply(scenario, service_context={})
            if not payload:
                continue

            # Latency injection
            delay_ms = payload.get("delay_ms", 0)
            if delay_ms:
                logger.info(
                    "Anomaly [%s] injecting %sms delay",
                    scenario,
                    delay_ms,
                )
                time.sleep(delay_ms / 1000.0)

            # Error-rate injection (database_cascade)
            error_rate = payload.get("error_rate", 0.0)
            if error_rate > 0 and random.random() < error_rate:
                logger.warning(
                    "Anomaly [%s] injecting random error (rate=%.1f)",
                    scenario,
                    error_rate,
                )
                raise HTTPException(
                    status_code=503,
                    detail=f"Simulated failure: {scenario}",
                )

            # Security attack — block writes
            if scenario == "security_attack" and payload.get("block_ip"):
                logger.warning("Anomaly [security_attack] blocking request")
                raise HTTPException(
                    status_code=403,
                    detail="Access denied due to active security anomaly",
                )

            # Config drift — disable order creation
            if scenario == "config_drift":
                features = payload.get("features_disabled", [])
                if "rate_limit" in features:
                    logger.warning("Anomaly [config_drift] rate-limit active")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.post("", response_model=Order, summary="Create a new order")
def create_order_endpoint(
    data: OrderCreate,
    engine: AnomalyEngine = Depends(_get_engine),
) -> Order:
    """Create an order via saga orchestration.

    The saga persists the order locally, publishes a ``order.created``
    event to Kafka, and transitions the status to ``confirmed``.
    """
    start = time.time()
    _check_anomaly(engine)

    order = create_order_saga(data)

    meter = get_meter("omniwatch.order_service")
    counter = meter.create_counter(
        name="omniwatch.orders.created",
        unit="1",
        description="Number of orders created",
    )
    counter.add(1)

    hist = meter.create_histogram(
        name="omniwatch.orders.request_duration",
        unit="ms",
        description="Request duration in milliseconds",
    )
    hist.record((time.time() - start) * 1000)

    return order


@router.get("", response_model=list[Order], summary="List all orders")
def list_orders_endpoint(engine: AnomalyEngine = Depends(_get_engine)) -> list[Order]:
    """Return every order currently stored in memory."""
    start = time.time()
    _check_anomaly(engine)

    orders = list_orders()

    meter = get_meter("omniwatch.order_service")
    counter = meter.create_counter(
        name="omniwatch.orders.listed",
        unit="1",
        description="Number of order list operations",
    )
    counter.add(1)

    hist = meter.create_histogram(
        name="omniwatch.orders.request_duration",
        unit="ms",
        description="Request duration in milliseconds",
    )
    hist.record((time.time() - start) * 1000)

    return orders


@router.get("/{order_id}", response_model=Order, summary="Get order by ID")
def get_order_endpoint(order_id: str, engine: AnomalyEngine = Depends(_get_engine)) -> Order:
    """Retrieve a single order by its unique identifier."""
    start = time.time()
    _check_anomaly(engine)

    order = get_order(order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")

    meter = get_meter("omniwatch.order_service")
    counter = meter.create_counter(
        name="omniwatch.orders.retrieved",
        unit="1",
        description="Number of orders retrieved (single)",
    )
    counter.add(1)

    hist = meter.create_histogram(
        name="omniwatch.orders.request_duration",
        unit="ms",
        description="Request duration in milliseconds",
    )
    hist.record((time.time() - start) * 1000)

    return order


@router.get(
    "/users/{user_id}",
    response_model=list[Order],
    summary="List orders by user",
)
def list_user_orders_endpoint(user_id: str, engine: AnomalyEngine = Depends(_get_engine)) -> list[Order]:
    """Return all orders placed by a specific user."""
    start = time.time()
    _check_anomaly(engine)

    orders = list_orders_by_user(user_id)

    meter = get_meter("omniwatch.order_service")
    counter = meter.create_counter(
        name="omniwatch.orders.listed",
        unit="1",
        description="Number of order list operations",
    )
    counter.add(1)

    hist = meter.create_histogram(
        name="omniwatch.orders.request_duration",
        unit="ms",
        description="Request duration in milliseconds",
    )
    hist.record((time.time() - start) * 1000)

    return orders
