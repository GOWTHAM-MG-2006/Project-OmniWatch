"""
OmniWatch — Phase 1 Test Configuration
Purpose: Shared fixtures for Phase 1 microservice tests — FastAPI test clients,
         OTel mocking, and sample data factories.

Usage:
    pytest tests/phase-1-microservices/ -v
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Ensure the project root is on sys.path so "services" is importable
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Isolated service loader — prevents module namespace collisions
# ---------------------------------------------------------------------------
_FLAT_MODULES = frozenset({"models", "crud", "routes", "saga", "kafka_client",
                           "middleware", "main"})
_SERVICE_PKGS = frozenset({"api_gateway", "user_service", "order_service"})


def _load_service_app(service_name: str):
    """Import a service's FastAPI app, evicting stale modules first.

    Only manages sys.path if the calling test module has not already set it
    (via ``_service_path``).  Returns the FastAPI application object.
    """
    for mod_name in list(sys.modules):
        parts = mod_name.split(".")
        if (len(parts) == 1 and parts[0] in _FLAT_MODULES) or (
            len(parts) >= 2 and parts[0] == "services" and parts[1] in _SERVICE_PKGS
        ):
            del sys.modules[mod_name]

    svc_dir = str(PROJECT_ROOT / "services" / service_name)
    added = svc_dir not in sys.path
    if added:
        sys.path.insert(0, svc_dir)
    try:
        return importlib.import_module(f"services.{service_name}.main").app
    finally:
        if added:  # only remove what we added — module fixture manages its own
            sys.path.remove(svc_dir)


# ---------------------------------------------------------------------------
# Per-module sys.path isolation — flat imports resolve to the correct service
# ---------------------------------------------------------------------------

_SERVICE_NAME_MAP = {
    "test_user_service": "user_service",
    "test_order_service": "order_service",
    "test_api_gateway": "api_gateway",
}


@pytest.fixture(scope="module", autouse=True)
def _service_path(request):
    """Add the relevant service directory to sys.path before any test in a
    service-specific test module runs, and evict stale flat modules so that
    ``from models import …`` inside CRUD / model files resolves to the
    _correct_ service's module.

    *test_user_service* → *services/user_service/* added to sys.path
    *test_order_service* → *services/order_service/* added to sys.path
    *test_api_gateway*   → *services/api_gateway/* added to sys.path
    """
    fname = Path(request.module.__file__ or "")
    service_name = _SERVICE_NAME_MAP.get(fname.stem)
    if service_name is None:
        yield
        return

    # Evict stale flat modules and service sub-packages so fresh imports
    # pick up the right files from the dir we are about to expose.
    for mod_name in list(sys.modules):
        parts = mod_name.split(".")
        if (len(parts) == 1 and parts[0] in _FLAT_MODULES) or (
            len(parts) >= 2 and parts[0] == "services" and parts[1] in _SERVICE_PKGS
        ):
            del sys.modules[mod_name]

    svc_dir = str(PROJECT_ROOT / "services" / service_name)
    sys.path.insert(0, svc_dir)
    yield
    if svc_dir in sys.path:
        sys.path.remove(svc_dir)


# ---------------------------------------------------------------------------
# Per-test CRUD store isolation — prevents state leakage across tests
# ---------------------------------------------------------------------------


def _clear_service_store(service_name: str) -> None:
    """Reset in-memory store dicts (``_users``, ``_orders``, etc.) inside a
    service's CRUD module so tests do not observe stale state from previous
    test functions within the same test module.

    No-op if the CRUD module has not been imported yet.
    """
    full_name = f"services.{service_name}.crud"
    mod = sys.modules.get(full_name)
    if mod is None:
        return
    for attr in tuple(vars(mod)):
        if attr.startswith("_") and isinstance(getattr(mod, attr), dict):
            setattr(mod, attr, {})


@pytest.fixture(autouse=True)
def _clear_crud_stores(request: pytest.FixtureRequest):
    """Autouse fixture that resets CRUD store dicts before every test function
    in a service-specific test module (``test_user_service``, ``test_order_service``).
    """
    fname = Path(request.module.__file__ or "")
    service_name = _SERVICE_NAME_MAP.get(fname.stem)
    if service_name is None:
        yield
        return
    _clear_service_store(service_name)
    yield
    _clear_service_store(service_name)


# ---------------------------------------------------------------------------
# Global OTel patch — prevents any service from connecting to otelcol:4317
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session", autouse=True)
def _patch_otel():
    """Replace OTel initialisation with a no-op for all tests in this session.

    Without this patch, ``init_otel()`` tries to open a gRPC connection to
    ``otelcol:4317``, which is not available in unit test environments.
    """
    with patch("services.common.otel_setup.init_otel") as mock:
        mock.return_value = None
        yield


# ---------------------------------------------------------------------------
# Sample data fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def user_data() -> dict:
    """Sample payload for creating a user."""
    return {"name": "Alice", "email": "alice@example.com"}


@pytest.fixture
def user_data_bob() -> dict:
    """Another sample user payload."""
    return {"name": "Bob", "email": "bob@example.com"}


@pytest.fixture
def order_data() -> dict:
    """Sample payload for creating a single-item order."""
    return {
        "user_id": "user-alice-001",
        "items": [
            {
                "product_id": "prod-widget",
                "name": "Super Widget",
                "quantity": 2,
                "price": 9.99,
            }
        ],
    }


@pytest.fixture
def order_multi_item_data() -> dict:
    """Sample payload for a multi-item order."""
    return {
        "user_id": "user-alice-001",
        "items": [
            {"product_id": "prod-a", "name": "Item A", "quantity": 1, "price": 19.99},
            {"product_id": "prod-b", "name": "Item B", "quantity": 3, "price": 4.50},
        ],
    }


# ---------------------------------------------------------------------------
# FastAPI TestClient fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def user_client():
    from fastapi.testclient import TestClient
    with TestClient(_load_service_app("user_service")) as client:
        yield client


@pytest.fixture(scope="function")
def order_client():
    from fastapi.testclient import TestClient

    app = _load_service_app("order_service")

    async def _fake_validate_user_exists(*args, **kwargs) -> None:
        """Test stub — user-service is not reachable in unit tests.

        ``_validate_user_exists`` performs an httpx GET to
        ``http://user-service:8001`` (the Docker network name), which does
        not resolve from the host where pytest runs. Stub it out so order
        route tests exercise the saga without a live user-service.
        """
        return

    with (
        patch(
            "routes._validate_user_exists",
            _fake_validate_user_exists,
        ),
        TestClient(app) as client,
    ):
        yield client


@pytest.fixture(scope="function")
def gateway_client():
    from fastapi.testclient import TestClient
    with TestClient(_load_service_app("api_gateway")) as client:
        yield client


# ---------------------------------------------------------------------------
# AnomalyEngine fixture for direct unit tests
# ---------------------------------------------------------------------------

@pytest.fixture
def engine():
    """Fresh ``AnomalyEngine`` instance (no active anomalies)."""
    from services.common.anomaly_injector import AnomalyEngine

    eng = AnomalyEngine(service_name="test-service")
    yield eng
    eng.clear_all()
