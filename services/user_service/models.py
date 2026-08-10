"""
OmniWatch — User Service
Component: Pydantic Models
Phase: 1
Purpose: User data models for CRUD operations
Inputs: API request bodies
Outputs: User schemas for responses and persistence
"""

from pydantic import BaseModel


class User(BaseModel):
    """Complete user record returned by the API."""

    id: str
    name: str
    email: str
    created_at: str


class UserCreate(BaseModel):
    """Request model for creating a new user."""

    name: str
    email: str


class UserUpdate(BaseModel):
    """Request model for updating an existing user (all fields optional)."""

    name: str | None = None
    email: str | None = None
