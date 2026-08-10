"""
OmniWatch — User Service
Component: In-Memory CRUD
Phase: 1
Purpose: In-memory storage layer for user entities with UUID-based IDs
Inputs: UserCreate data from routes
Outputs: User model instances
"""

import uuid
from datetime import datetime, timezone

from models import User, UserCreate, UserUpdate

# In-memory user store: user_id -> User
_users: dict[str, User] = {}


def create_user(data: UserCreate) -> User:
    """Create a new user with auto-generated UUID and ISO timestamp.

    Args:
        data: User creation payload (name, email)

    Returns:
        Newly created User instance
    """
    user_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    user = User(id=user_id, name=data.name, email=data.email, created_at=now)
    _users[user_id] = user
    return user


def get_user(user_id: str) -> User | None:
    """Retrieve a user by their UUID.

    Args:
        user_id: The user's UUID string

    Returns:
        User if found, None otherwise
    """
    return _users.get(user_id)


def list_users() -> list[User]:
    """Return all stored users.

    Returns:
        List of all User records (empty list if none exist)
    """
    return list(_users.values())


def delete_user(user_id: str) -> bool:
    """Delete a user by UUID.

    Args:
        user_id: The user's UUID string

    Returns:
        True if a user was deleted, False if not found
    """
    if user_id in _users:
        del _users[user_id]
        return True
    return False


def update_user(user_id: str, data: UserUpdate) -> User | None:
    """Update a user's mutable fields (name, email) by UUID.

    Only fields present in the request body are changed; the user_id and
    created_at are never modified.

    Args:
        user_id: The user's UUID string
        data: Update payload with optional name/email fields

    Returns:
        Updated User if found, None otherwise
    """
    user = _users.get(user_id)
    if user is None:
        return None
    updates = data.model_dump(exclude_unset=True)
    updated = user.model_copy(update=updates)
    _users[user_id] = updated
    return updated
