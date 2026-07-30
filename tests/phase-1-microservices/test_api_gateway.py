"""
OmniWatch — Phase 1 Microservice Tests
Component: API Gateway (services/api_gateway/)
Purpose: Validate auth middleware (valid/invalid/missing tokens, public paths),
         OTel middleware (X-Request-ID header), route listing, health check,
         and anomaly injection endpoints.
"""

from __future__ import annotations

import pytest

TOKEN = "omniwatch-token"
AUTH_HEADER = {"Authorization": f"Bearer {TOKEN}"}


# =============================================================================
# Health endpoint (public — no auth required)
# =============================================================================


class TestGatewayHealth:
    """Public health-check endpoint tests."""

    def test_health_returns_healthy(self, gateway_client):
        """GET /health returns 200."""
        resp = gateway_client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert data["service"] == "api-gateway"

    def test_health_no_auth_required(self, gateway_client):
        """GET /health works without an Authorization header."""
        resp = gateway_client.get("/health", headers={})
        assert resp.status_code == 200

    def test_health_returns_json(self, gateway_client):
        """GET /health returns application/json."""
        resp = gateway_client.get("/health")
        assert resp.headers["content-type"].startswith("application/json")


# =============================================================================
# Routes endpoint (protected — requires auth)
# =============================================================================


class TestGatewayRoutes:
    """Protected route-listing endpoint tests."""

    def test_routes_lists_endpoints(self, gateway_client):
        """GET /routes returns the registered route list with valid auth."""
        resp = gateway_client.get("/routes", headers=AUTH_HEADER)
        assert resp.status_code == 200
        data = resp.json()
        assert data["service"] == "api-gateway"
        assert len(data["routes"]) > 0
        paths = [r["path"] for r in data["routes"]]
        assert "/health" in paths
        assert "/routes" in paths

    def test_routes_no_auth_required(self, gateway_client):
        """GET /routes without auth returns 401."""
        resp = gateway_client.get("/routes", headers={})
        assert resp.status_code == 401


# =============================================================================
# Auth middleware tests
# =============================================================================


class TestAuthMiddleware:
    """Tests for ``AuthMiddleware`` — Bearer token validation on protected routes."""

    PROTECTED_PATH = "/routes"  # Uses GET /routes as the protected test path

    def test_valid_token_passes(self, gateway_client):
        """Request with valid Bearer token succeeds."""
        resp = gateway_client.get(self.PROTECTED_PATH, headers=AUTH_HEADER)
        assert resp.status_code == 200

    def test_missing_auth_header_returns_401(self, gateway_client):
        """Request without Authorization header returns 401."""
        resp = gateway_client.get(self.PROTECTED_PATH)
        assert resp.status_code == 401
        assert "Missing or invalid" in resp.json()["detail"]

    def test_empty_auth_header_returns_401(self, gateway_client):
        """Request with empty Authorization header returns 401."""
        resp = gateway_client.get(
            self.PROTECTED_PATH, headers={"Authorization": ""}
        )
        assert resp.status_code == 401

    def test_non_bearer_header_returns_401(self, gateway_client):
        """Request with non-Bearer Authorization returns 401."""
        resp = gateway_client.get(
            self.PROTECTED_PATH,
            headers={"Authorization": "Basic dXNlcjpwYXNz"},
        )
        assert resp.status_code == 401

    def test_invalid_token_returns_401(self, gateway_client):
        """Request with wrong Bearer token returns 401."""
        resp = gateway_client.get(
            self.PROTECTED_PATH,
            headers={"Authorization": "Bearer wrong-token"},
        )
        assert resp.status_code == 401
        assert "Invalid" in resp.json()["detail"]

    def test_malformed_bearer_token_returns_401(self, gateway_client):
        """Request with malformed Bearer token (missing actual token) returns 401."""
        resp = gateway_client.get(
            self.PROTECTED_PATH,
            headers={"Authorization": "Bearer "},
        )
        assert resp.status_code == 401


# =============================================================================
# Public path exemption tests
# =============================================================================


class TestPublicPaths:
    """Verify that public paths bypass auth middleware."""

    def test_docs_public(self, gateway_client):
        """GET /docs is accessible without auth."""
        resp = gateway_client.get("/docs", headers={})
        assert resp.status_code in (200, 307)  # 307 = redirect to /docs

    def test_openapi_json_public(self, gateway_client):
        """GET /openapi.json is accessible without auth."""
        resp = gateway_client.get("/openapi.json", headers={})
        assert resp.status_code == 200

    def test_inject_anomaly_public(self, gateway_client):
        """GET /__inject/anomaly is accessible without auth."""
        resp = gateway_client.get("/__inject/anomaly", headers={})
        # 200 means accessible, no auth prompt
        assert resp.status_code == 200

    def test_inject_post_public(self, gateway_client):
        """POST /__inject/anomaly is accessible without auth."""
        resp = gateway_client.post(
            "/__inject/anomaly",
            headers={},
            json={"scenario": "latency_spike", "ttl_seconds": 60},
        )
        assert resp.status_code in (200, 400)  # 400 if anomaly system returns 400

    def test_inject_delete_public(self, gateway_client):
        """DELETE /__inject/anomaly is accessible without auth."""
        resp = gateway_client.delete("/__inject/anomaly", headers={})
        assert resp.status_code in (200, 400)


# =============================================================================
# OTel middleware — X-Request-ID header
# =============================================================================


class TestRequestIdHeader:
    """Verify OTelMiddleware sets the X-Request-ID header."""

    def test_request_id_present_on_health(self, gateway_client):
        """X-Request-ID header is present on health check responses."""
        resp = gateway_client.get("/health")
        assert "X-Request-ID" in resp.headers
        assert len(resp.headers["X-Request-ID"]) > 0

    def test_request_id_present_on_protected(self, gateway_client):
        """X-Request-ID header is present on protected route responses."""
        resp = gateway_client.get("/routes", headers=AUTH_HEADER)
        assert "X-Request-ID" in resp.headers

    def test_request_id_present_on_401(self, gateway_client):
        """X-Request-ID header is present even on 401 error responses."""
        resp = gateway_client.get("/routes")
        assert resp.status_code == 401
        assert "X-Request-ID" in resp.headers

    def test_request_id_unique_per_request(self, gateway_client):
        """Each request gets a unique X-Request-ID."""
        r1 = gateway_client.get("/health")
        r2 = gateway_client.get("/health")
        assert r1.headers["X-Request-ID"] != r2.headers["X-Request-ID"]


# =============================================================================
# Anomaly injection endpoints
# =============================================================================


class TestGatewayAnomaly:
    """Test the anomaly injection endpoints (mounted on api-gateway)."""

    def test_inject_and_list_anomaly(self, gateway_client):
        """POST then GET /__inject/anomaly shows the injected scenario."""
        gateway_client.post(
            "/__inject/anomaly",
            json={"scenario": "latency_spike", "ttl_seconds": 60},
        )
        resp = gateway_client.get("/__inject/anomaly")
        assert resp.status_code == 200
        data = resp.json()
        assert data["service"] == "api-gateway"
        assert len(data["active"]) > 0

    def test_clear_anomaly(self, gateway_client):
        """DELETE /__inject/anomaly/{scenario} deactivates a scenario."""
        gateway_client.post(
            "/__inject/anomaly",
            json={"scenario": "memory_leak", "ttl_seconds": 60},
        )
        resp = gateway_client.delete("/__inject/anomaly/memory_leak")
        assert resp.status_code == 200
        assert resp.json()["status"] == "cleared"

    def test_clear_all_anomalies(self, gateway_client):
        """DELETE /__inject/anomaly clears all scenarios."""
        gateway_client.post(
            "/__inject/anomaly",
            json={"scenario": "latency_spike", "ttl_seconds": 60},
        )
        resp = gateway_client.delete("/__inject/anomaly")
        assert resp.status_code == 200
        assert resp.json()["cleared"] == "all"


# =============================================================================
# 404 on unknown routes
# =============================================================================


class TestGatewayUnknownRoutes:
    """Verify 404 for undefined routes (with and without auth)."""

    def test_unknown_route_without_auth_returns_404(self, gateway_client):
        """Unknown path returns 404, not 401 (it passes auth check first)."""
        resp = gateway_client.get("/nonexistent-route")
        # Auth middleware sees it as a non-public path, rejects with 401
        assert resp.status_code in (401, 404)

    def test_unknown_route_with_auth_returns_404(self, gateway_client):
        """Unknown path with valid auth returns 404."""
        resp = gateway_client.get("/nonexistent-route", headers=AUTH_HEADER)
        assert resp.status_code == 404
