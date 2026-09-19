"""OmniWatch Workspace tests — model/provisioning/isolation (Wave 2, todo 2)."""

from __future__ import annotations

import jwt as pyjwt
import pytest
from fastapi.testclient import TestClient


def _register(client: TestClient, email: str, password: str = "correct-horse-2024!") -> dict:
    resp = client.post("/auth/register", json={"email": email, "password": password})
    assert resp.status_code == 201, resp.text
    return resp.json()


def _login(client: TestClient, email: str, password: str = "correct-horse-2024!") -> dict:
    resp = client.post("/auth/login", json={"email": email, "password": password})
    assert resp.status_code == 200, resp.text
    return resp.json()


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _create(client: TestClient, token: str, name: str, **extra) -> dict:
    resp = client.post("/workspaces", json={"name": name, **extra}, headers=_auth(token))
    assert resp.status_code == 201, resp.text
    return resp.json()


# ---------------------------------------------------------------------------
# Happy path: create -> list -> switch -> rename -> delete-tombstone
# ---------------------------------------------------------------------------


def test_create_list_switch_rename_delete(client: TestClient) -> None:
    reg = _register(client, "ws@example.com")
    pair = _login(client, "ws@example.com")

    created = _create(client, pair["access_token"], "Acme App")
    ws = created["workspace"]
    assert ws["slug"] == "acme-app"
    assert created["connection_bundle"]["kafka_topic_prefix"] == "ws_acme-app."
    assert created["connection_bundle"]["clickhouse_database"] == "omniwatch_ws_acme-app"
    assert created["connection_bundle"]["minio_prefix"] == "workspaces/acme-app/"
    assert ws["neo4j_workspace"] == "acme-app"
    assert ws["k8s_namespace"] == "omniwatch-ws-acme-app"
    assert created["provisioning"]["row"] == "skipped (memory store)"
    assert "secret" not in str(created["connection_bundle"]).lower()

    listed = client.get("/workspaces", headers=_auth(pair["access_token"]))
    assert listed.status_code == 200, listed.text
    assert [w["workspace_id"] for w in listed.json()] == [ws["workspace_id"]]

    switch = client.post(
        f"/workspaces/{ws['workspace_id']}/switch", headers=_auth(pair["access_token"]))
    assert switch.status_code == 200, switch.text
    claims = pyjwt.decode(switch.json()["access_token"], options={"verify_signature": False})
    assert claims["ws"] == ws["workspace_id"]
    assert claims["sub"] == reg["user_id"]
    assert switch.json()["workspace_id"] == ws["workspace_id"]

    renamed = client.patch(
        f"/workspaces/{ws['workspace_id']}",
        json={"name": "Acme Renamed"},
        headers=_auth(pair["access_token"]),
    )
    assert renamed.status_code == 200, renamed.text
    assert renamed.json()["name"] == "Acme Renamed"
    assert renamed.json()["slug"] == "acme-app"  # slug immutable

    deleted = client.delete(
        f"/workspaces/{ws['workspace_id']}", headers=_auth(pair["access_token"]))
    assert deleted.status_code == 200, deleted.text
    assert deleted.json() == {"workspace_id": ws["workspace_id"], "deleted": True}

    # Tombstoned: excluded from list, single-get/switch behave as absent (403).
    assert client.get("/workspaces", headers=_auth(pair["access_token"])).json() == []
    assert client.get(
        f"/workspaces/{ws['workspace_id']}", headers=_auth(pair["access_token"])).status_code == 403
    assert client.post(
        f"/workspaces/{ws['workspace_id']}/switch",
        headers=_auth(pair["access_token"])).status_code == 403


def test_switch_tracks_active_across_three_workspaces(client: TestClient) -> None:
    _register(client, "multi@example.com")
    pair = _login(client, "multi@example.com")
    ids = [_create(client, pair["access_token"], f"App {i}")["workspace"]["workspace_id"]
           for i in range(3)]
    for wid in ids:
        resp = client.post(f"/workspaces/{wid}/switch", headers=_auth(pair["access_token"]))
        assert resp.status_code == 200, resp.text
        claims = pyjwt.decode(resp.json()["access_token"], options={"verify_signature": False})
        assert claims["ws"] == wid


# ---------------------------------------------------------------------------
# Failure matrix: dup 409, cross-user 403, malformed slug 422
# ---------------------------------------------------------------------------


def test_duplicate_name_409_per_user(client: TestClient) -> None:
    _register(client, "dup@example.com")
    pair = _login(client, "dup@example.com")
    _create(client, pair["access_token"], "Same Name")
    dup = client.post(
        "/workspaces", json={"name": "Same Name"}, headers=_auth(pair["access_token"]))
    assert dup.status_code == 409, dup.text


def test_same_name_different_users_ok_slug_suffix(client: TestClient) -> None:
    _register(client, "u1@example.com")
    _register(client, "u2@example.com")
    t1, t2 = _login(client, "u1@example.com"), _login(client, "u2@example.com")
    w1 = _create(client, t1["access_token"], "Shared")["workspace"]
    w2 = _create(client, t2["access_token"], "Shared")["workspace"]
    assert w1["slug"] == "shared"
    assert w2["slug"] == "shared-2"  # global slug collision -> suffix, not 409


def test_cross_user_access_403_never_404(client: TestClient) -> None:
    _register(client, "owner@example.com")
    _register(client, "intruder@example.com")
    owner_t = _login(client, "owner@example.com")["access_token"]
    evil_t = _login(client, "intruder@example.com")["access_token"]
    wid = _create(client, owner_t, "Private")["workspace"]["workspace_id"]
    assert client.get(f"/workspaces/{wid}", headers=_auth(evil_t)).status_code == 403
    assert client.patch(
        f"/workspaces/{wid}", json={"name": "Hijack"},
        headers=_auth(evil_t)).status_code == 403
    assert client.delete(f"/workspaces/{wid}", headers=_auth(evil_t)).status_code == 403
    assert client.post(
        f"/workspaces/{wid}/switch", headers=_auth(evil_t)).status_code == 403
    # Unknown id: same 403 (no existence oracle).
    assert client.get("/workspaces/00000000-0000-0000-0000-000000000000",
                       headers=_auth(evil_t)).status_code == 403
    # Intruder's own list never contains the other's workspace.
    assert client.get("/workspaces", headers=_auth(evil_t)).json() == []


def test_malformed_slug_422(client: TestClient) -> None:
    _register(client, "slug@example.com")
    pair = _login(client, "slug@example.com")
    for bad in ("Bad Slug!", "UPPER", "has space", "-lead", ""):
        resp = client.post(
            "/workspaces", json={"name": "Ok Name", "slug": bad},
            headers=_auth(pair["access_token"]))
        assert resp.status_code == 422, (bad, resp.text)


def test_rename_duplicate_409(client: TestClient) -> None:
    _register(client, "rn@example.com")
    pair = _login(client, "rn@example.com")
    _create(client, pair["access_token"], "First")
    second = _create(client, pair["access_token"], "Second")["workspace"]
    resp = client.patch(
        f"/workspaces/{second['workspace_id']}", json={"name": "First"},
        headers=_auth(pair["access_token"]))
    assert resp.status_code == 409, resp.text


# ---------------------------------------------------------------------------
# Default bootstrap: unauthenticated single-tenant behavior byte-identical
# ---------------------------------------------------------------------------


def test_default_bootstrap_unauthenticated(client: TestClient) -> None:
    listed = client.get("/workspaces")
    assert listed.status_code == 200, listed.text
    workspaces = listed.json()
    assert len(workspaces) == 1
    default = workspaces[0]
    assert default["slug"] == "default"
    assert default["kafka_topic_prefix"] == ""
    assert default["clickhouse_database"] == "omniwatch"
    assert default["minio_prefix"] == ""
    assert default["k8s_namespace"] == "omniwatch"

    single = client.get(f"/workspaces/{default['workspace_id']}")
    assert single.status_code == 200, single.text
    assert single.json()["slug"] == "default"

    switch = client.post(f"/workspaces/{default['workspace_id']}/switch")
    assert switch.status_code == 200, switch.text
    claims = pyjwt.decode(switch.json()["access_token"], options={"verify_signature": False})
    assert claims["ws"] == default["workspace_id"]

    # Second boot call is idempotent: still exactly one default.
    assert len(client.get("/workspaces").json()) == 1


def test_isolation_naming_contract() -> None:
    from storage.config import (
        workspace_database,
        workspace_kafka_prefix,
        workspace_k8s_namespace,
        workspace_prefix,
        workspace_topic,
    )

    assert workspace_kafka_prefix("acme") == "ws_acme."
    assert workspace_kafka_prefix("default") == ""
    assert workspace_topic("acme", "omniwatch.anomalies.detected") == (
        "ws_acme.omniwatch.anomalies.detected")
    assert workspace_topic("default", "omniwatch.anomalies.detected") == (
        "omniwatch.anomalies.detected")
    assert workspace_database("acme") == "omniwatch_ws_acme"
    assert workspace_database("default") == "omniwatch"
    assert workspace_prefix("acme") == "workspaces/acme/"
    assert workspace_prefix("default") == ""
    assert workspace_k8s_namespace("acme") == "omniwatch-ws-acme"
    assert workspace_k8s_namespace("default") == "omniwatch"
