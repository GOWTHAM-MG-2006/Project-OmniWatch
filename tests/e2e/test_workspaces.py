"""
OmniWatch — Entry-Point E2E
Component: Workspace isolation proofs (ENTRY-5)
Phase: entry-point (Wave 3, todo 5)
Purpose: userA/userB x 2 workspaces isolation matrix at API level
         (Kafka topics, ClickHouse rows, MinIO objects, Neo4j nodes)
         + UI-backing switch/URL-tamper + default legacy compat.
Inputs: In-process identity + dashboard TestClients, scope-keyed fake backends
Outputs: pytest verdicts; per-direction proof table (see .omo evidence file)
"""

from __future__ import annotations

import os
import re

# Test-only env BEFORE any identity/dashboard import (module app factories
# configure at import time). Never prod, never real secrets.
os.environ.setdefault("OMNIWATCH_ENV", "dev")
os.environ.setdefault(
    "OMNIWATCH_JWT_SECRET", "test-only-jwt-secret-not-a-credential")
os.environ.setdefault("OMNIWATCH_IDENTITY_STORE", "memory")

from types import SimpleNamespace  # noqa: E402

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import dashboard.api.main as dash_main  # noqa: E402
from dashboard.api.main import create_app as create_dashboard_app  # noqa: E402
from dashboard.api.workspace_scope import (  # noqa: E402
    query_opa_tenant_allow,
    set_membership_resolver,
    tenant_allow,
)
from identity.main import create_app as create_identity_app  # noqa: E402
from identity.settings import IdentitySettings  # noqa: E402
from identity.store import MemoryStore  # noqa: E402
from identity.workspaces import get_registry  # noqa: E402

PASSWORD = "correct-horse-2024!"
USER_A = "alice.entry5@example.com"
USER_B = "bob.entry5@example.com"
TAGS = ("A1", "A2", "B1", "B2")
_DB_RE = re.compile(r"FROM\s+`?([A-Za-z0-9_\-]+)`?\.")


# ---------------------------------------------------------------------------
# Scope-keyed fake backends (partition exactly like the real isolation keys:
# ClickHouse per-workspace DB, MinIO workspaces/<slug>/ prefix, Neo4j slug).
# ---------------------------------------------------------------------------


class _FakeCHResult:
    def __init__(self, markers: list[str]) -> None:
        self.column_names = [("marker",)]
        self.result_rows = [(m,) for m in markers]


class _FakeCHClient:
    """Fake ClickHouse client keyed by the DB qualifier in the SQL text."""

    def __init__(self, seed: dict[str, list[str]]) -> None:
        self.seed = seed
        self.queries: list[str] = []

    def query(self, sql: str, parameters: dict | None = None) -> _FakeCHResult:
        self.queries.append(sql)
        match = _DB_RE.search(sql)
        db = match.group(1) if match else ""
        return _FakeCHResult(list(self.seed.get(db, [])))


class _FakeMinIOClient:
    """Fake MinIO client keyed by full object key (prefix-filtered list)."""

    def __init__(self, keys: list[str]) -> None:
        self.keys = keys

    def list_objects(
        self, bucket: str, prefix: str = "", recursive: bool = False
    ) -> list:
        return [
            SimpleNamespace(object_name=k) for k in self.keys
            if k.startswith(prefix)
        ]


class _FakeNeo4jSession:
    def __init__(self, seed: dict[str, list[dict]]) -> None:
        self.seed = seed

    def __enter__(self) -> "_FakeNeo4jSession":
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def run(self, cypher: str, parameters: dict | None = None) -> list[dict]:
        slug = (parameters or {}).get("ws_slug", "default")
        return [dict(row) for row in self.seed.get(slug, [])]


class _FakeNeo4jDriver:
    def __init__(self, seed: dict[str, list[dict]]) -> None:
        self._session = _FakeNeo4jSession(seed)

    def session(self) -> _FakeNeo4jSession:
        return self._session


# ---------------------------------------------------------------------------
# Module fixture: 2 users x 2 workspaces + fakes + registry-backed resolver.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def iso():
    """Build the isolated world: identity app, 4 workspaces, fakes, resolver."""
    settings = IdentitySettings.from_env()
    id_app = create_identity_app(store=MemoryStore(), settings=settings)
    ic = TestClient(id_app)

    users: dict[str, dict] = {}
    for email in (USER_A, USER_B):
        reg = ic.post(
            "/auth/register", json={"email": email, "password": PASSWORD})
        assert reg.status_code == 201, reg.text
        login = ic.post(
            "/auth/login", json={"email": email, "password": PASSWORD})
        assert login.status_code == 200, login.text
        users[email] = login.json()

    plan = ((USER_A, "alpha-one", "A1"), (USER_A, "alpha-two", "A2"),
            (USER_B, "beta-one", "B1"), (USER_B, "beta-two", "B2"))
    wid: dict[str, str] = {}
    slug: dict[str, str] = {}
    for email, name, tag in plan:
        resp = ic.post(
            "/workspaces", json={"name": name},
            headers={"Authorization": f"Bearer {users[email]['access_token']}"},
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()["workspace"]
        wid[tag] = body["workspace_id"]
        slug[tag] = body["slug"]

    tok: dict[str, str] = {}
    for email, tag in ((USER_A, "A1"), (USER_A, "A2"),
                       (USER_B, "B1"), (USER_B, "B2")):
        resp = ic.post(
            f"/workspaces/{wid[tag]}/switch",
            headers={"Authorization": f"Bearer {users[email]['access_token']}"},
        )
        assert resp.status_code == 200, resp.text
        tok[tag] = resp.json()["access_token"]

    # Seed fakes with distinct markers per isolation key.
    ch_seed = {f"omniwatch_ws_{slug[t]}": [f"marker-ch-{t}"] for t in TAGS}
    ch_seed["omniwatch"] = ["marker-ch-default"]
    fake_ch = _FakeCHClient(ch_seed)
    minio_keys = [f"workspaces/{slug[t]}/marker-{t}.json" for t in TAGS]
    minio_keys.append("legacy-default.json")
    fake_minio = _FakeMinIOClient(minio_keys)
    neo_seed = {
        slug[t]: [{"id": f"node-{t}", "name": f"node-{t}",
                   "labels": ["Service"]}] for t in TAGS
    }
    neo_seed["default"] = [{"id": "node-legacy", "name": "node-legacy",
                            "labels": ["Service"]}]
    fake_neo = _FakeNeo4jDriver(neo_seed)

    def _registry_resolver(
        user_id: str, workspace_id: str, auth_header: str | None
    ) -> str | None:
        record = get_registry().get_owned(workspace_id, user_id)
        return record.slug if record is not None else None

    orig_ch = dash_main._get_ch_client
    orig_minio = dash_main._get_minio_client
    orig_neo = dash_main._get_neo4j_driver
    dash_main._get_ch_client = lambda: fake_ch  # noqa: E731
    dash_main._get_minio_client = lambda: fake_minio  # noqa: E731
    dash_main._get_neo4j_driver = lambda: fake_neo  # noqa: E731
    set_membership_resolver(_registry_resolver)
    try:
        dash = TestClient(create_dashboard_app())
        yield {
            "ic": ic, "dash": dash, "users": users, "tok": tok,
            "wid": wid, "slug": slug, "ch": fake_ch,
        }
    finally:
        dash_main._get_ch_client = orig_ch
        dash_main._get_minio_client = orig_minio
        dash_main._get_neo4j_driver = orig_neo
        set_membership_resolver(None)


def _auth(tag: str, iso: dict) -> dict[str, str]:
    return {"Authorization": f"Bearer {iso['tok'][tag]}"}


# ---------------------------------------------------------------------------
# 1. Scope positives + Kafka topic mapping (topics layer, all 4 workspaces).
# ---------------------------------------------------------------------------


def test_scope_positive_and_kafka_topics(iso: dict) -> None:
    """Each token resolves to its own slug/DB/topic-prefix/MinIO-prefix."""
    for tag in TAGS:
        resp = iso["dash"].get("/api/scope", headers=_auth(tag, iso))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["slug"] == iso["slug"][tag], body
        assert body["ch_database"] == f"omniwatch_ws_{iso['slug'][tag]}", body
        assert body["kafka_prefix"] == f"ws_{iso['slug'][tag]}.", body
        assert body["minio_prefix"] == f"workspaces/{iso['slug'][tag]}/", body
        assert body["neo4j_workspace"] == iso["slug"][tag], body
        expected_topic = (
            f"ws_{iso['slug'][tag]}.omniwatch.anomalies.detected")
        assert body["kafka_topics"]["anomalies"] == expected_topic, body
        assert resp.headers.get("X-Workspace-Scope") == iso["slug"][tag]


# ---------------------------------------------------------------------------
# 2-4. Partition matrix: A1<->A2, B1<->B2 invisibility at 3 data layers.
# ---------------------------------------------------------------------------


def test_ch_partition_matrix(iso: dict) -> None:
    """Scope X never sees markers of any other scope (ClickHouse rows)."""
    for tag in TAGS:
        resp = iso["dash"].get("/api/metrics", headers=_auth(tag, iso))
        assert resp.status_code == 200, resp.text
        text = resp.text
        assert f"marker-ch-{tag}" in text, text
        for other in TAGS:
            if other != tag:
                assert f"marker-ch-{other}" not in text, (tag, other, text)
        assert "marker-ch-default" not in text, text


def test_minio_partition_matrix(iso: dict) -> None:
    """Scope X never sees objects of any other scope (MinIO prefix)."""
    for tag in TAGS:
        resp = iso["dash"].get("/api/audit-logs", headers=_auth(tag, iso))
        assert resp.status_code == 200, resp.text
        files = resp.json()["audit_logs"]
        assert any(f"marker-{tag}.json" in f for f in files), files
        for other in TAGS:
            if other != tag:
                assert not any(
                    f"marker-{other}.json" in f for f in files), (tag, other)
        assert "legacy-default.json" not in files, files


def test_neo4j_partition_matrix(iso: dict) -> None:
    """Scope X never sees nodes of any other scope (Neo4j :Workspace)."""
    for tag in TAGS:
        resp = iso["dash"].get("/api/entities", headers=_auth(tag, iso))
        assert resp.status_code == 200, resp.text
        ids = [row.get("id") for row in resp.json()["entities"]]
        assert f"node-{tag}" in ids, ids
        for other in TAGS:
            if other != tag:
                assert f"node-{other}" not in ids, (tag, other, ids)
        assert "node-legacy" not in ids, ids


# ---------------------------------------------------------------------------
# 5. Cross-user 403 matrix: A->B x2, B->A x2 (never 404-leak, never data).
# ---------------------------------------------------------------------------


def test_cross_user_403_matrix(iso: dict) -> None:
    """Explicit cross-user scope requests are 403 with no data leak."""
    probes = (("A1", "B1"), ("A1", "B2"), ("B1", "A1"), ("B1", "A2"))
    for from_tag, to_tag in probes:
        resp = iso["dash"].get(
            "/api/scope",
            params={"workspace_id": iso["wid"][to_tag]},
            headers=_auth(from_tag, iso),
        )
        assert resp.status_code == 403, (from_tag, to_tag, resp.text)
        assert "marker" not in resp.text.lower(), resp.text
        # Same denial on a data endpoint (tamper before data, not just scope).
        data = iso["dash"].get(
            "/api/metrics",
            params={"workspace_id": iso["wid"][to_tag]},
            headers=_auth(from_tag, iso),
        )
        assert data.status_code == 403, (from_tag, to_tag, data.text)


# ---------------------------------------------------------------------------
# 6. Auth edge cases: 401 vs 403 vs 422 discipline.
# ---------------------------------------------------------------------------


def test_auth_edge_discipline(iso: dict) -> None:
    """Tampered -> 401; unknown -> 403 (no oracle); malformed -> 422."""
    dash = iso["dash"]
    good = iso["tok"]["A1"]
    bad = good[:-1] + ("a" if good[-1] != "a" else "b")
    resp = dash.get("/api/scope", headers={"Authorization": f"Bearer {bad}"})
    assert resp.status_code == 401, resp.text
    # Refresh token presented as access -> 401 (wrong type).
    refresh = iso["users"][USER_A]["refresh_token"]
    resp = dash.get(
        "/api/scope", headers={"Authorization": f"Bearer {refresh}"})
    assert resp.status_code == 401, resp.text
    # Non-Bearer scheme -> 401.
    resp = dash.get("/api/scope", headers={"Authorization": f"Token {good}"})
    assert resp.status_code == 401, resp.text
    # Well-formed but unknown workspace_id -> 403 (never 404-leak).
    resp = dash.get(
        "/api/scope",
        params={"workspace_id": "00000000-0000-0000-0000-000000000000"},
        headers=_auth("A1", iso),
    )
    assert resp.status_code == 403, resp.text
    # Malformed workspace_id -> 422 (path traversal / garbage rejected).
    for evil in ("../evil", "..\\evil", "!!", ""):
        resp = dash.get(
            "/api/scope",
            params={"workspace_id": evil},
            headers=_auth("A1", iso),
        )
        assert resp.status_code == 422, (evil, resp.text)
    # Unauthenticated + explicit scope -> 403 (cannot prove membership).
    resp = dash.get(
        "/api/scope", params={"workspace_id": iso["wid"]["A1"]})
    assert resp.status_code == 403, resp.text


# ---------------------------------------------------------------------------
# 7. UI level: switch (own 200 + context changes) + URL tamper (403).
# ---------------------------------------------------------------------------


def test_switch_and_url_tamper(iso: dict) -> None:
    """Backing calls the UI makes: own switch works, cross access blocked."""
    ic, dash = iso["ic"], iso["dash"]
    legacy_a = iso["users"][USER_A]["access_token"]
    # Switch to own second workspace -> 200, dashboard honors the new JWT.
    resp = ic.post(
        f"/workspaces/{iso['wid']['A2']}/switch",
        headers={"Authorization": f"Bearer {legacy_a}"},
    )
    assert resp.status_code == 200, resp.text
    switched = resp.json()["access_token"]
    scope = dash.get(
        "/api/scope", headers={"Authorization": f"Bearer {switched}"})
    assert scope.status_code == 200, scope.text
    assert scope.json()["slug"] == iso["slug"]["A2"], scope.text
    metrics = dash.get(
        "/api/metrics", headers={"Authorization": f"Bearer {switched}"})
    assert "marker-ch-A2" in metrics.text, metrics.text
    assert "marker-ch-A1" not in metrics.text, metrics.text
    # Query-param switch to own other workspace -> 200 (same data context).
    via_param = dash.get(
        "/api/metrics",
        params={"workspace_id": iso["wid"]["A2"]},
        headers=_auth("A1", iso),
    )
    assert via_param.status_code == 200, via_param.text
    assert "marker-ch-A2" in via_param.text, via_param.text
    # URL tamper: GET /workspaces/<other-user-id> -> 403 (what the UI calls).
    tamper = ic.get(
        f"/workspaces/{iso['wid']['B1']}",
        headers={"Authorization": f"Bearer {legacy_a}"},
    )
    assert tamper.status_code == 403, tamper.text
    # Switch to another user's workspace -> 403.
    cross_switch = ic.post(
        f"/workspaces/{iso['wid']['B1']}/switch",
        headers={"Authorization": f"Bearer {legacy_a}"},
    )
    assert cross_switch.status_code == 403, cross_switch.text


# ---------------------------------------------------------------------------
# 8. tenant_allow gate: local mirror matrix + live OPA parity when reachable.
# ---------------------------------------------------------------------------


def test_tenant_allow_gate() -> None:
    """Mirror matches tenant_allow.rego; live OPA agrees when reachable."""
    assert tenant_allow("u1", "acme", "acme", "u1") is True
    assert tenant_allow("u1", "acme", "other", "u1") is False
    assert tenant_allow("u2", "acme", "acme", "u1") is False
    assert tenant_allow("u1", "default", "default", "u1") is True
    assert tenant_allow("u1", "", "default", "anyone") is True
    live = query_opa_tenant_allow("u1", "acme", "acme", "u1")
    if live is not None:
        assert live is True, "live OPA disagrees with tenant_allow mirror"
        assert query_opa_tenant_allow("u2", "acme", "acme", "u1") is False


# ---------------------------------------------------------------------------
# 9. Legacy compat: unauthenticated boot stays byte-identical (default).
# ---------------------------------------------------------------------------


def test_default_legacy_boot(iso: dict) -> None:
    """No token -> default scope, bare names, legacy markers, no regression."""
    dash = iso["dash"]
    scope = dash.get("/api/scope")
    assert scope.status_code == 200, scope.text
    body = scope.json()
    assert body["slug"] == "default", body
    assert body["ch_database"] == "omniwatch", body
    assert body["kafka_prefix"] == "", body
    assert body["minio_prefix"] == "", body
    assert body["kafka_topics"]["anomalies"] == "omniwatch.anomalies.detected"
    assert scope.headers.get("X-Workspace-Scope") == "default"
    iso["ch"].queries.clear()
    resp = dash.get("/api/metrics")
    assert resp.status_code == 200, resp.text
    assert "marker-ch-default" in resp.text, resp.text
    assert iso["ch"].queries, "expected a ClickHouse query to be issued"
    last_sql = iso["ch"].queries[-1]
    assert "FROM omniwatch.metrics" in last_sql, last_sql
    assert "omniwatch_ws_" not in last_sql, last_sql
    logs = dash.get("/api/audit-logs")
    assert logs.status_code == 200, logs.text
    assert "legacy-default.json" in logs.json()["audit_logs"]
    ents = dash.get("/api/entities")
    assert ents.status_code == 200, ents.text
    assert "node-legacy" in [r.get("id") for r in ents.json()["entities"]]
