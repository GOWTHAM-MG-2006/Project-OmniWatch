"""
OmniWatch — Phase 1 Microservice Tests
Component: User Service (services/user_service/)
Purpose: Validate user CRUD operations, API routes, health check, error handling,
         and anomaly injection integration.
"""

from __future__ import annotations

import pytest


# =============================================================================
# CRUD unit tests
# =============================================================================


class TestUserCrud:
    """Direct unit tests against the in-memory CRUD layer."""

    def test_create_user(self, user_data):
        """create_user() returns a new user with generated id and timestamp."""
        from services.user_service.crud import create_user
        from services.user_service.models import UserCreate

        data = UserCreate(**user_data)
        user = create_user(data)
        assert user.id is not None
        assert user.name == "Alice"
        assert user.email == "alice@example.com"
        assert user.created_at is not None

    def test_get_user_returns_user(self, user_data):
        """get_user() returns the correct user by id."""
        from services.user_service.crud import create_user, get_user
        from services.user_service.models import UserCreate

        created = create_user(UserCreate(**user_data))
        fetched = get_user(created.id)
        assert fetched is not None
        assert fetched.id == created.id
        assert fetched.name == "Alice"

    def test_get_user_returns_none_for_missing(self):
        """get_user() returns None for a non-existent id."""
        from services.user_service.crud import get_user

        assert get_user("nonexistent-id") is None

    def test_list_users(self, user_data, user_data_bob):
        """list_users() returns all created users."""
        from services.user_service.crud import create_user, list_users
        from services.user_service.models import UserCreate

        create_user(UserCreate(**user_data))
        create_user(UserCreate(**user_data_bob))
        users = list_users()
        assert len(users) == 2

    def test_list_users_empty_initially(self):
        """list_users() returns an empty list before any users are created."""
        from services.user_service.crud import list_users

        assert list_users() == []

    def test_delete_user_removes_user(self, user_data):
        """delete_user() returns True and removes the user."""
        from services.user_service.crud import create_user, delete_user, get_user
        from services.user_service.models import UserCreate

        user = create_user(UserCreate(**user_data))
        assert delete_user(user.id) is True
        assert get_user(user.id) is None

    def test_delete_user_returns_false_for_missing(self):
        """delete_user() returns False for a non-existent id."""
        from services.user_service.crud import delete_user

        assert delete_user("nonexistent-id") is False

    def test_create_user_generates_unique_ids(self, user_data):
        """Each create_user() call generates a unique id."""
        from services.user_service.crud import create_user
        from services.user_service.models import UserCreate

        u1 = create_user(UserCreate(**user_data))
        u2 = create_user(UserCreate(**{**user_data, "email": "bob@example.com"}))
        assert u1.id != u2.id


# =============================================================================
# Route integration tests
# =============================================================================


class TestUserServiceRoutes:
    """Test user-service endpoints via FastAPI TestClient."""

    # -- Health ----------------------------------------------------------

    def test_health_endpoint(self, user_client):
        """GET /health returns 200 with service status."""
        resp = user_client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert data["service"] == "user-service"

    # -- Create user ----------------------------------------------------

    def test_create_user(self, user_client, user_data):
        """POST /api/v1/users/ creates a user and returns 201."""
        resp = user_client.post("/api/v1/users/", json=user_data)
        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "Alice"
        assert data["email"] == "alice@example.com"
        assert "id" in data
        assert "created_at" in data

    def test_create_user_validates_body(self, user_client):
        """POST /api/v1/users/ with missing fields returns 422."""
        resp = user_client.post("/api/v1/users/", json={"name": "No Email"})
        assert resp.status_code == 422  # missing email

    # -- List users -----------------------------------------------------

    def test_list_users_empty(self, user_client):
        """GET /api/v1/users/ returns [] when no users exist."""
        resp = user_client.get("/api/v1/users/")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_list_users_with_data(self, user_client, user_data, user_data_bob):
        """GET /api/v1/users/ returns all users."""
        user_client.post("/api/v1/users/", json=user_data)
        user_client.post("/api/v1/users/", json=user_data_bob)
        resp = user_client.get("/api/v1/users/")
        assert resp.status_code == 200
        users = resp.json()
        assert len(users) == 2

    # -- Get user -------------------------------------------------------

    def test_get_user(self, user_client, user_data):
        """GET /api/v1/users/{id} returns the correct user."""
        created = user_client.post("/api/v1/users/", json=user_data).json()
        resp = user_client.get(f"/api/v1/users/{created['id']}")
        assert resp.status_code == 200
        assert resp.json()["id"] == created["id"]

    def test_get_user_404(self, user_client):
        """GET /api/v1/users/{id} returns 404 for unknown id."""
        resp = user_client.get("/api/v1/users/nonexistent-id")
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"].lower()

    # -- Delete user ----------------------------------------------------

    def test_delete_user(self, user_client, user_data):
        """DELETE /api/v1/users/{id} returns 204 and removes the user."""
        created = user_client.post("/api/v1/users/", json=user_data).json()
        resp = user_client.delete(f"/api/v1/users/{created['id']}")
        assert resp.status_code == 204

        # Verify deleted
        get_resp = user_client.get(f"/api/v1/users/{created['id']}")
        assert get_resp.status_code == 404

    def test_delete_user_404(self, user_client):
        """DELETE /api/v1/users/{id} returns 404 for unknown id."""
        resp = user_client.delete("/api/v1/users/nonexistent-id")
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"].lower()

    # -- Full lifecycle -------------------------------------------------

    def test_user_full_lifecycle(self, user_client, user_data, user_data_bob):
        """End-to-end: create multiple users, list, fetch, delete."""
        # Create 2 users
        u1 = user_client.post("/api/v1/users/", json=user_data).json()
        u2 = user_client.post("/api/v1/users/", json=user_data_bob).json()

        # List all
        all_users = user_client.get("/api/v1/users/").json()
        assert len(all_users) == 2

        # Get each
        assert user_client.get(f"/api/v1/users/{u1['id']}").status_code == 200
        assert user_client.get(f"/api/v1/users/{u2['id']}").status_code == 200

        # Delete one
        user_client.delete(f"/api/v1/users/{u1['id']}")
        assert user_client.get(f"/api/v1/users/{u1['id']}").status_code == 404
        assert user_client.get(f"/api/v1/users/{u2['id']}").status_code == 200


# =============================================================================
# Anomaly injection integration
# =============================================================================


class TestUserServiceAnomaly:
    """Test anomaly injection endpoints on user-service."""

    def test_inject_anomaly(self, user_client):
        """POST /__inject/anomaly activates a scenario."""
        resp = user_client.post(
            "/__inject/anomaly",
            json={"scenario": "latency_spike", "ttl_seconds": 60},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "injected"

    def test_list_active_anomalies(self, user_client):
        """GET /__inject/anomaly lists active scenarios."""
        user_client.post(
            "/__inject/anomaly",
            json={"scenario": "latency_spike", "ttl_seconds": 60},
        )
        resp = user_client.get("/__inject/anomaly")
        assert resp.status_code == 200
        data = resp.json()
        assert data["service"] == "user-service"
        assert len(data["active"]) == 1
        assert data["active"][0]["scenario"] == "latency_spike"

    def test_clear_anomaly(self, user_client):
        """DELETE /__inject/anomaly/{scenario} deactivates it."""
        user_client.post(
            "/__inject/anomaly",
            json={"scenario": "memory_leak", "ttl_seconds": 60},
        )
        resp = user_client.delete("/__inject/anomaly/memory_leak")
        assert resp.status_code == 200
        assert resp.json()["status"] == "cleared"

    def test_clear_all_anomalies(self, user_client):
        """DELETE /__inject/anomaly clears all."""
        user_client.post(
            "/__inject/anomaly",
            json={"scenario": "latency_spike", "ttl_seconds": 60},
        )
        resp = user_client.delete("/__inject/anomaly")
        assert resp.status_code == 200
        assert resp.json()["cleared"] == "all"
