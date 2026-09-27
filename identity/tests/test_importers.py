"""OmniWatch Importer credential tests — issuance/GET/rotate/403 (exporter-importer-split, A1).

NOTE: registration is disabled (static-user mode), so these tests log in as
the seeded static admin (admin/Admin123) instead of registering. The
cross-user case creates the second user directly in the store fixture.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from identity.auth import hash_password
from identity.store import MemoryStore

ADMIN_EMAIL = "admin"
ADMIN_PASSWORD = "Admin123"


def _admin_login(client: TestClient) -> dict:
    resp = client.post(
        "/auth/login", json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
    assert resp.status_code == 200, resp.text
    return resp.json()


def _login(client: TestClient, email: str, password: str) -> dict:
    resp = client.post("/auth/login", json={"email": email, "password": password})
    assert resp.status_code == 200, resp.text
    return resp.json()


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _create(client: TestClient, token: str, name: str) -> dict:
    resp = client.post("/workspaces", json={"name": name}, headers=_auth(token))
    assert resp.status_code == 201, resp.text
    return resp.json()


def test_create_issues_importer_endpoint_and_token_once(client: TestClient) -> None:
    pair = _admin_login(client)

    created = _create(client, pair["access_token"], "Importer App")
    assert created["importer_endpoint"], "creation must issue an importer endpoint"
    assert created["importer_endpoint"].endswith("/ingest")
    assert created["importer_token"], "creation must issue the raw token once"
    assert len(created["importer_token"]) >= 32

    # Raw token is write-only: GET info exposes endpoint but never the token.
    ws_id = created["workspace"]["workspace_id"]
    info = client.get(f"/importers/by-workspace/{ws_id}", headers=_auth(pair["access_token"]))
    assert info.status_code == 200, info.text
    assert info.json()["endpoint_url"] == created["importer_endpoint"]
    assert info.json().get("api_token") in (None, ""), \
        "creation path keeps the token write-only on later reads"
    assert "token_hash" not in info.text


def test_rotate_returns_new_token_same_endpoint(client: TestClient) -> None:
    pair = _admin_login(client)
    created = _create(client, pair["access_token"], "Rotate App")
    ws_id = created["workspace"]["workspace_id"]

    rotated = client.post(f"/importers/{ws_id}/rotate", headers=_auth(pair["access_token"]))
    assert rotated.status_code == 200, rotated.text
    body = rotated.json()
    assert body["endpoint_url"] == created["importer_endpoint"]
    assert body["api_token"]
    assert body["api_token"] != created["importer_token"]


def test_cross_user_importer_access_is_403(client: TestClient, store: MemoryStore) -> None:
    owner = _admin_login(client)
    created = _create(client, owner["access_token"], "Owned App")
    ws_id = created["workspace"]["workspace_id"]

    # Second user created directly (registration is disabled by design).
    stranger_pw = "stranger-proof-99!"
    store.create_user("stranger@example.com", hash_password(stranger_pw))
    stranger = _login(client, "stranger@example.com", stranger_pw)

    info = client.get(f"/importers/by-workspace/{ws_id}", headers=_auth(stranger["access_token"]))
    assert info.status_code == 403, info.text

    rotated = client.post(f"/importers/{ws_id}/rotate", headers=_auth(stranger["access_token"]))
    assert rotated.status_code == 403, rotated.text

    unknown = client.get("/importers/by-workspace/does-not-exist", headers=_auth(stranger["access_token"]))
    assert unknown.status_code == 403, unknown.text


def test_verify_resolves_token_to_workspace(client: TestClient) -> None:
    pair = _admin_login(client)
    created = _create(client, pair["access_token"], "Verify App")
    ws_id = created["workspace"]["workspace_id"]

    ok = client.post(
        "/importers/verify",
        json={"token": created["importer_token"]},
        headers={"X-Internal-Secret": "dev-only-internal-secret"},
    )
    assert ok.status_code == 200, ok.text
    assert ok.json()["workspace_id"] == ws_id
    assert ok.json()["workspace_slug"] == created["workspace"]["slug"]

    bad_token = client.post(
        "/importers/verify",
        json={"token": "not-a-real-token"},
        headers={"X-Internal-Secret": "dev-only-internal-secret"},
    )
    assert bad_token.status_code == 403, bad_token.text

    bad_secret = client.post(
        "/importers/verify",
        json={"token": created["importer_token"]},
        headers={"X-Internal-Secret": "wrong-secret"},
    )
    assert bad_secret.status_code == 403, bad_secret.text

    no_secret = client.post("/importers/verify", json={"token": created["importer_token"]})
    assert no_secret.status_code == 403, no_secret.text


def test_backfill_reveals_token_once_then_never(client: TestClient) -> None:
    from identity.importers import reset_importer_registry

    pair = _admin_login(client)
    created = _create(client, pair["access_token"], "Backfill App")
    ws_id = created["workspace"]["workspace_id"]
    headers = _auth(pair["access_token"])

    # Simulate a pre-split workspace: importer row gone, workspace owned.
    reset_importer_registry()

    first = client.get(f"/importers/by-workspace/{ws_id}", headers=headers)
    assert first.status_code == 200, first.text
    assert first.json()["endpoint_url"].endswith("/ingest")
    assert first.json()["api_token"], "first read must reveal the token once"

    second = client.get(f"/importers/by-workspace/{ws_id}", headers=headers)
    assert second.status_code == 200, second.text
    assert second.json().get("api_token") in (None, ""), \
        "later reads must never repeat the token"


LIVE_BASE = "https://discussed-shanghai-automobiles-judge.trycloudflare.com"
_INTERNAL = {"X-Internal-Secret": "dev-only-internal-secret"}


def test_internal_endpoint_push_updates_existing_and_new(client: TestClient) -> None:
    pair = _admin_login(client)
    headers = _auth(pair["access_token"])
    created = _create(client, pair["access_token"], "Push App")
    ws_id = created["workspace"]["workspace_id"]
    old_endpoint = created["importer_endpoint"]

    pushed = client.post(
        "/importers/internal/endpoint", json={"base_url": LIVE_BASE}, headers=_INTERNAL)
    assert pushed.status_code == 200, pushed.text
    assert pushed.json()["updated"] >= 1
    assert pushed.json()["endpoint_url"] == f"{LIVE_BASE}/ingest"

    # Existing row follows the fresh URL; token secrecy untouched.
    info = client.get(f"/importers/by-workspace/{ws_id}", headers=headers)
    assert info.status_code == 200, info.text
    assert info.json()["endpoint_url"] == f"{LIVE_BASE}/ingest"
    assert info.json()["endpoint_url"] != old_endpoint
    assert info.json().get("api_token") in (None, "")

    # New workspaces are born on the live URL too (no container rebuild).
    created2 = _create(client, pair["access_token"], "Push App Two")
    assert created2["importer_endpoint"] == f"{LIVE_BASE}/ingest"

    # Idempotent re-push of the same URL.
    again = client.post(
        "/importers/internal/endpoint", json={"base_url": LIVE_BASE + "/"}, headers=_INTERNAL)
    assert again.status_code == 200, again.text
    assert again.json()["endpoint_url"] == f"{LIVE_BASE}/ingest"


def test_internal_endpoint_rejects_bad_secret_and_scheme(client: TestClient) -> None:
    bad = client.post(
        "/importers/internal/endpoint",
        json={"base_url": LIVE_BASE},
        headers={"X-Internal-Secret": "wrong-secret"},
    )
    assert bad.status_code == 403, bad.text

    missing = client.post("/importers/internal/endpoint", json={"base_url": LIVE_BASE})
    assert missing.status_code == 403, missing.text

    scheme = client.post(
        "/importers/internal/endpoint",
        json={"base_url": "ftp://not-a-tunnel"},
        headers=_INTERNAL,
    )
    assert scheme.status_code == 400, scheme.text
