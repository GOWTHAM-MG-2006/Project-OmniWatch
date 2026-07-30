"""
OmniWatch — Order Service
Component: Data Models
Phase: 1
Purpose: Pydantic v2 models for order entities and API request/response schemas
Inputs: API request bodies, CRUD layer data
Outputs: Serialized Order, OrderItem, and related schemas
"""

from pydantic import BaseModel, Field


class OrderItem(BaseModel):
    """A single line item within an order."""

    product_id: str
    name: str
    quantity: int = Field(ge=1, description="Quantity must be at least 1")
    price: float = Field(ge=0.0, description="Price must be non-negative")


class OrderItemCreate(BaseModel):
    """Payload for creating a new line item in an order."""

    product_id: str
    name: str
    quantity: int = Field(ge=1, description="Quantity must be at least 1")
    price: float = Field(ge=0.0, description="Price must be non-negative")


class Order(BaseModel):
    """Full order representation with identity and status."""

    id: str
    user_id: str
    items: list[OrderItem]
    total: float
    status: str
    created_at: str


class OrderCreate(BaseModel):
    """Payload for creating a new order."""

    user_id: str
    items: list[OrderItemCreate]
