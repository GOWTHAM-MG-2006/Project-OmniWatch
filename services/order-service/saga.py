"""
OmniWatch — Order Service
Component: Saga Orchestration
Phase: 1
Purpose: Simple orchestration for order creation — creates order, publishes
         Kafka event, updates status to "confirmed".
Inputs: OrderCreate payload
Outputs: Confirmed Order (or order stuck at "pending" on error)
"""

import logging

from models import Order, OrderCreate
from crud import create_order, update_order_status
from kafka_client import get_default_producer

logger = logging.getLogger("omniwatch.order_service.saga")

ORDER_EVENTS_TOPIC = "omniwatch.orders.events"


def create_order_saga(data: OrderCreate) -> Order:
    """Run the order-creation saga.

    Steps:
        1. Create the order locally with status ``"pending"``.
        2. Publish an ``order.created`` event to Kafka.
        3. Update the order status to ``"confirmed"``.

    If the Kafka publish fails the order remains at ``"pending"`` status
    — it can be reconciled later or retried manually.

    Returns:
        The final Order (status ``"confirmed"`` on success, ``"pending"``
        if the event could not be published).
    """
    # Step 1 — persist with pending status
    order = create_order(data)
    logger.info("Order created locally: order_id=%s status=pending", order.id)

    # Step 2 — publish event
    producer = get_default_producer()
    event = {
        "event_type": "order.created",
        "order_id": order.id,
        "user_id": order.user_id,
        "total": order.total,
        "item_count": len(order.items),
        "status": order.status,
        "timestamp": order.created_at,
    }
    producer.publish(ORDER_EVENTS_TOPIC, key=order.id, value=event)

    # Step 3 — confirm if event published
    confirmed = update_order_status(order.id, "confirmed")
    if confirmed is not None:
        logger.info("Order confirmed: order_id=%s status=confirmed", order.id)
        return confirmed

    # Fallback — if update_order_status returned None (shouldn't happen)
    logger.error("Order status update failed for order_id=%s", order.id)
    return order
