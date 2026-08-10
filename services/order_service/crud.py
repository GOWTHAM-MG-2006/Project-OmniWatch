"""
OmniWatch — Order Service
Component: In-Memory CRUD
Phase: 1
Purpose: In-memory storage and CRUD operations for Order entities
Inputs: OrderCreate payloads from saga layer
Outputs: Order model instances
"""

import uuid
from datetime import datetime, timezone

from models import Order, OrderCreate, OrderItem

# In-memory store: order_id -> Order
_orders: dict[str, Order] = {}


def create_order(data: OrderCreate) -> Order:
    """Create a new order with generated UUID and ISO timestamp.

    The order is stored with status "pending" and returned to the caller.
    """
    order_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    total = round(sum(item.price * item.quantity for item in data.items), 2)

    order = Order(
        id=order_id,
        user_id=data.user_id,
        items=[
            OrderItem(
                product_id=item.product_id,
                name=item.name,
                quantity=item.quantity,
                price=item.price,
            )
            for item in data.items
        ],
        total=total,
        status="pending",
        created_at=now,
    )

    _orders[order_id] = order
    return order


def get_order(order_id: str) -> Order | None:
    """Retrieve an order by its ID, or None if not found."""
    return _orders.get(order_id)


def list_orders() -> list[Order]:
    """Return all orders (newest first)."""
    return list(_orders.values())


def list_orders_by_user(user_id: str) -> list[Order]:
    """Return all orders for a given user (newest first)."""
    return [o for o in _orders.values() if o.user_id == user_id]


def update_order_status(order_id: str, status: str) -> Order | None:
    """Update the status of an existing order. Returns None if not found."""
    order = _orders.get(order_id)
    if order is None:
        return None
    updated = order.model_copy(update={"status": status})
    _orders[order_id] = updated
    return updated
