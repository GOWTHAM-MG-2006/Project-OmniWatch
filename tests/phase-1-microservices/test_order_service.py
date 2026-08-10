"""
OmniWatch — Phase 1 Microservice Tests
Component: Order Service (services/order_service/)
Purpose: Validate order CRUD operations, saga orchestration, API routes, health check,
         model validation, Kafka client graceful degradation, and anomaly injection.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

# =============================================================================
# Model validation tests
# =============================================================================


class TestOrderModels:
    """Unit tests for Pydantic model validation."""

    def test_order_item_valid(self):
        """A valid OrderItem passes validation."""
        from services.order_service.models import OrderItem

        item = OrderItem(product_id="p1", name="Item", quantity=2, price=10.0)
        assert item.product_id == "p1"
        # total is computed at order level, not on OrderItem

    def test_order_item_quantity_must_be_positive(self):
        """OrderItem quantity must be >= 1."""
        from services.order_service.models import OrderItem

        with pytest.raises(ValidationError):
            OrderItem(product_id="p1", name="Item", quantity=0, price=10.0)

    def test_order_item_price_must_be_non_negative(self):
        """OrderItem price must be >= 0."""
        from services.order_service.models import OrderItem

        with pytest.raises(ValidationError):
            OrderItem(product_id="p1", name="Item", quantity=1, price=-5.0)

    def test_order_create_valid(self, order_data):
        """A valid OrderCreate passes validation."""
        from services.order_service.models import OrderCreate

        data = OrderCreate(**order_data)
        assert len(data.items) == 1
        assert data.items[0].name == "Super Widget"

    def test_order_create_validates_items_required(self):
        """OrderCreate requires items."""
        from services.order_service.models import OrderCreate

        with pytest.raises(ValidationError):
            OrderCreate(user_id="u1", items=[])

    def test_order_has_total(self):
        """Order model computes/retains total."""
        from services.order_service.models import Order, OrderItem

        order = Order(
            id="o1",
            user_id="u1",
            items=[OrderItem(product_id="p1", name="A", quantity=2, price=10.0)],
            total=20.0,
            status="pending",
            created_at="2026-01-01T00:00:00Z",
        )
        assert order.total == 20.0
        assert order.status == "pending"


# =============================================================================
# CRUD unit tests
# =============================================================================


class TestOrderCrud:
    """Direct unit tests against the in-memory CRUD layer."""

    def test_create_order(self, order_data):
        """create_order() stores the order with pending status."""
        from services.order_service.crud import create_order
        from services.order_service.models import OrderCreate

        data = OrderCreate(**order_data)
        order = create_order(data)
        assert order.id is not None
        assert order.user_id == "user-alice-001"
        assert order.status == "pending"
        assert order.total == 19.98  # 2 * 9.99
        assert len(order.items) == 1

    def test_get_order(self, order_data):
        """get_order() returns the correct order."""
        from services.order_service.crud import create_order, get_order
        from services.order_service.models import OrderCreate

        created = create_order(OrderCreate(**order_data))
        fetched = get_order(created.id)
        assert fetched is not None
        assert fetched.id == created.id

    def test_get_order_none_for_missing(self):
        """get_order() returns None for a non-existent id."""
        from services.order_service.crud import get_order

        assert get_order("nonexistent") is None

    def test_list_orders(self, order_data, order_multi_item_data):
        """list_orders() returns all orders."""
        from services.order_service.crud import create_order, list_orders
        from services.order_service.models import OrderCreate

        create_order(OrderCreate(**order_data))
        create_order(OrderCreate(**order_multi_item_data))
        orders = list_orders()
        assert len(orders) == 2

    def test_list_orders_empty_initially(self):
        """list_orders() is empty before any orders."""
        from services.order_service.crud import list_orders

        assert list_orders() == []

    def test_list_orders_by_user(self, order_data):
        """list_orders_by_user() filters by user_id."""
        from services.order_service.crud import create_order, list_orders_by_user
        from services.order_service.models import OrderCreate

        create_order(OrderCreate(**order_data))
        orders_alice = list_orders_by_user("user-alice-001")
        assert len(orders_alice) == 1
        orders_bob = list_orders_by_user("user-bob-001")
        assert orders_bob == []

    def test_update_order_status(self, order_data):
        """update_order_status() changes the order's status."""
        from services.order_service.crud import (
            create_order,
            get_order,
            update_order_status,
        )
        from services.order_service.models import OrderCreate

        created = create_order(OrderCreate(**order_data))
        updated = update_order_status(created.id, "confirmed")
        assert updated is not None
        assert updated.status == "confirmed"
        # Verify persistence
        fetched = get_order(created.id)
        assert fetched is not None
        assert fetched.status == "confirmed"

    def test_update_order_status_none_for_missing(self):
        """update_order_status() returns None for unknown id."""
        from services.order_service.crud import update_order_status

        assert update_order_status("nonexistent", "confirmed") is None

    def test_total_calculation(self, order_multi_item_data):
        """create_order() correctly sums item prices."""
        from services.order_service.crud import create_order
        from services.order_service.models import OrderCreate

        order = create_order(OrderCreate(**order_multi_item_data))
        # (1 * 19.99) + (3 * 4.50) = 19.99 + 13.50 = 33.49
        assert order.total == 33.49


# =============================================================================
# Saga unit tests
# =============================================================================


class TestOrderSaga:
    """Unit tests for the order saga orchestration."""

    def test_saga_creates_confirmed_order(self, order_data):
        """create_order_saga() returns an order with 'confirmed' status."""
        from services.order_service.models import OrderCreate
        from services.order_service.saga import create_order_saga

        order = create_order_saga(OrderCreate(**order_data))
        assert order.id is not None
        assert order.status == "confirmed"
        assert order.total == 19.98

    def test_saga_increments_order_count(self, order_data):
        """Each saga call creates a distinct order."""
        from services.order_service.models import OrderCreate
        from services.order_service.saga import create_order_saga

        o1 = create_order_saga(OrderCreate(**order_data))
        o2 = create_order_saga(OrderCreate(**order_data))
        assert o1.id != o2.id

    def test_saga_sets_correct_user_id(self, order_data):
        """Saga preserves the user_id from the input."""
        from services.order_service.models import OrderCreate
        from services.order_service.saga import create_order_saga

        order = create_order_saga(OrderCreate(**order_data))
        assert order.user_id == "user-alice-001"


# =============================================================================
# Kafka client unit tests
# =============================================================================


class TestKafkaClient:
    """Test KafkaProducer graceful degradation when Kafka is unavailable."""

    def test_producer_initializes_without_kafka(self):
        """KafkaProducer constructor does not fail when Kafka is missing."""
        from services.order_service.kafka_client import KafkaProducer

        producer = KafkaProducer(bootstrap_servers="localhost:19092")
        assert producer.bootstrap_servers == "localhost:19092"

    def test_publish_noop_when_kafka_down(self):
        """publish() silently no-ops when Kafka is unavailable."""
        from services.order_service.kafka_client import KafkaProducer

        producer = KafkaProducer(bootstrap_servers="localhost:19092")
        # Should not raise any exception
        producer.publish("test-topic", "key1", {"event": "test"})

    def test_flush_noop_when_kafka_down(self):
        """flush() silently no-ops when Kafka is unavailable."""
        from services.order_service.kafka_client import KafkaProducer

        producer = KafkaProducer(bootstrap_servers="localhost:19092")
        producer.flush()  # Should not raise

    def test_singleton_get_default_producer(self):
        """get_default_producer() returns a KafkaProducer instance."""
        from services.order_service.kafka_client import get_default_producer

        producer = get_default_producer()
        assert producer is not None
        assert isinstance(producer.bootstrap_servers, str)


# =============================================================================
# Route integration tests
# =============================================================================


class TestOrderServiceRoutes:
    """Test order-service endpoints via FastAPI TestClient."""

    def test_health_endpoint(self, order_client):
        """GET /health returns 200."""
        resp = order_client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert data["service"] == "order-service"

    def test_create_order(self, order_client, order_data):
        """POST /api/v1/orders creates an order and returns it."""
        resp = order_client.post("/api/v1/orders", json=order_data)
        assert resp.status_code == 200
        data = resp.json()
        assert data["user_id"] == "user-alice-001"
        assert "id" in data
        # Saga should transition to confirmed (Kafka not available -> stays pending
        # but saga code attempts confirm; _ensure_connected fails, but
        # update_order_status is called regardless)
        assert data["status"] in ("confirmed", "pending")

    def test_create_order_validates_body(self, order_client):
        """POST /api/v1/orders with missing fields returns 422."""
        resp = order_client.post("/api/v1/orders", json={"user_id": "u1"})
        assert resp.status_code == 422

    def test_list_orders_empty(self, order_client):
        """GET /api/v1/orders returns [] when none exist."""
        resp = order_client.get("/api/v1/orders")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_list_orders_with_data(self, order_client, order_data):
        """GET /api/v1/orders returns all orders."""
        order_client.post("/api/v1/orders", json=order_data)
        resp = order_client.get("/api/v1/orders")
        assert resp.status_code == 200
        orders = resp.json()
        assert len(orders) == 1

    def test_get_order(self, order_client, order_data):
        """GET /api/v1/orders/{id} returns the correct order."""
        created = order_client.post("/api/v1/orders", json=order_data).json()
        resp = order_client.get(f"/api/v1/orders/{created['id']}")
        assert resp.status_code == 200
        assert resp.json()["id"] == created["id"]

    def test_get_order_404(self, order_client):
        """GET /api/v1/orders/{id} returns 404 for unknown id."""
        resp = order_client.get("/api/v1/orders/nonexistent")
        assert resp.status_code == 404

    def test_list_orders_by_user(self, order_client, order_data):
        """GET /api/v1/orders/users/{user_id} filters orders."""
        order_client.post("/api/v1/orders", json=order_data)
        resp = order_client.get("/api/v1/orders/users/user-alice-001")
        assert resp.status_code == 200
        orders = resp.json()
        assert len(orders) == 1
        assert orders[0]["user_id"] == "user-alice-001"

    def test_list_orders_by_user_empty(self, order_client):
        """GET /api/v1/orders/users/{user_id} returns [] for unknown user."""
        resp = order_client.get("/api/v1/orders/users/nobody")
        assert resp.status_code == 200
        assert resp.json() == []


# =============================================================================
# Anomaly injection integration
# =============================================================================


class TestOrderServiceAnomaly:
    """Test anomaly injection endpoints on order-service."""

    def test_inject_anomaly(self, order_client):
        """POST /__inject/anomaly works."""
        resp = order_client.post(
            "/__inject/anomaly",
            json={"scenario": "latency_spike", "ttl_seconds": 60},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "injected"

    def test_list_active_anomalies(self, order_client):
        """GET /__inject/anomaly lists active scenarios."""
        order_client.post(
            "/__inject/anomaly",
            json={"scenario": "config_drift", "ttl_seconds": 60},
        )
        resp = order_client.get("/__inject/anomaly")
        assert resp.status_code == 200
        data = resp.json()
        assert data["service"] == "order-service"
        assert len(data["active"]) == 1

    def test_clear_anomaly(self, order_client):
        """DELETE /__inject/anomaly/{scenario} works."""
        order_client.post(
            "/__inject/anomaly",
            json={"scenario": "security_attack", "ttl_seconds": 60},
        )
        resp = order_client.delete("/__inject/anomaly/security_attack")
        assert resp.status_code == 200
        assert resp.json()["status"] == "cleared"
