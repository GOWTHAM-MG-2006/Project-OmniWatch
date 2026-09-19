"""OmniWatch Identity tests — shared fixtures (in-memory store, test secret)."""

from __future__ import annotations

import os

# Test-only env BEFORE any identity import (module-level app configures at
# import time). Never prod, never real secrets.
os.environ["OMNIWATCH_ENV"] = "dev"
os.environ["OMNIWATCH_JWT_SECRET"] = "test-only-jwt-secret-not-a-credential"
os.environ["OMNIWATCH_IDENTITY_STORE"] = "memory"

import pytest
from fastapi.testclient import TestClient

from identity.main import create_app
from identity.settings import IdentitySettings
from identity.store import MemoryStore


@pytest.fixture()
def store() -> MemoryStore:
    """Fresh isolated store per test."""
    return MemoryStore()


@pytest.fixture()
def client(store: MemoryStore) -> TestClient:
    """TestClient wired to a fresh store + test secret."""
    settings = IdentitySettings.from_env()
    app = create_app(store=store, settings=settings)
    return TestClient(app)


@pytest.fixture()
def registered(client: TestClient) -> dict:
    """One registered user (email + user_id + password)."""
    email = "ada@example.com"
    password = "correct-horse-2024!"
    resp = client.post(
        "/auth/register", json={"email": email, "password": password})
    assert resp.status_code == 201, resp.text
    body = resp.json()
    body["password"] = password
    return body
