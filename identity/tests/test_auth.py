"""OmniWatch Identity tests — register/login/JWT contract (Wave 1, todo 1)."""

from __future__ import annotations

import jwt as pyjwt
import pytest
from fastapi.testclient import TestClient

from identity.main import create_app
from identity.settings import IdentitySettings
from identity.store import MemoryStore

INJECTION_EMAIL = "' OR 1=1--"


def _jwt_shape(token: str) -> bool:
    """JWT shape: header.payload.sig (three dot-separated segments)."""
    parts = token.split(".")
    return len(parts) == 3 and all(parts)


# ---------------------------------------------------------------------------
# Happy path: register -> login -> me -> refresh rotation -> replay fails
# ---------------------------------------------------------------------------


def test_register_login_me_refresh_rotation(client: TestClient) -> None:
    reg = client.post(
        "/auth/register",
        json={"email": "happy@example.com",
              "password": "sunshine-state-99!"},
    )
    assert reg.status_code == 201, reg.text
    user_id = reg.json()["user_id"]

    login = client.post(
        "/auth/login",
        json={"email": "happy@example.com",
              "password": "sunshine-state-99!"},
    )
    assert login.status_code == 200, login.text
    pair = login.json()
    assert _jwt_shape(pair["access_token"])
    assert _jwt_shape(pair["refresh_token"])
    assert pair["token_type"] == "bearer"

    # Access claims carry sub + ws + exp.
    claims = pyjwt.decode(
        pair["access_token"], options={"verify_signature": False})
    assert claims["sub"] == user_id
    assert claims["ws"] is None
    assert "exp" in claims

    me = client.get(
        "/auth/me", headers={"Authorization": f"Bearer {pair['access_token']}"})
    assert me.status_code == 200, me.text
    assert me.json()["user_id"] == user_id
    assert me.json()["email"] == "happy@example.com"

    # Refresh rotation: new pair issued...
    refresh = client.post(
        "/auth/refresh", json={"refresh_token": pair["refresh_token"]})
    assert refresh.status_code == 200, refresh.text
    pair2 = refresh.json()
    assert _jwt_shape(pair2["access_token"])
    assert pair2["refresh_token"] != pair["refresh_token"]

    # ...and replay of the OLD (revoked) refresh fails.
    replay = client.post(
        "/auth/refresh", json={"refresh_token": pair["refresh_token"]})
    assert replay.status_code == 401, replay.text


def test_logout_revokes_refresh(client: TestClient, registered: dict) -> None:
    login = client.post(
        "/auth/login",
        json={"email": registered["email"],
              "password": registered["password"]},
    )
    refresh_token = login.json()["refresh_token"]

    out = client.post("/auth/logout", json={"refresh_token": refresh_token})
    assert out.status_code == 200, out.text

    reuse = client.post(
        "/auth/refresh", json={"refresh_token": refresh_token})
    assert reuse.status_code == 401, reuse.text


def test_passwords_stored_bcrypt_hashed(
    client: TestClient, store: MemoryStore
) -> None:
    client.post(
        "/auth/register",
        json={"email": "hash@example.com",
              "password": "never-store-plain-11"},
    )
    record = store.get_user_by_email("hash@example.com")
    assert record is not None
    assert record.pw_hash != "never-store-plain-11"
    assert record.pw_hash.startswith("$2b$")


# ---------------------------------------------------------------------------
# Failure matrix: 409 / 401 / 422
# ---------------------------------------------------------------------------


def test_duplicate_email_409(client: TestClient) -> None:
    body = {"email": "dup@example.com", "password": "unique-enough-12"}
    assert client.post("/auth/register", json=body).status_code == 201
    dup = client.post("/auth/register", json=body)
    assert dup.status_code == 409, dup.text


def test_wrong_password_401(client: TestClient, registered: dict) -> None:
    bad = client.post(
        "/auth/login",
        json={"email": registered["email"], "password": "wrong-password-1"},
    )
    assert bad.status_code == 401, bad.text


def test_unknown_email_401(client: TestClient) -> None:
    missing = client.post(
        "/auth/login",
        json={"email": "nobody@example.com",
              "password": "some-password-1"},
    )
    assert missing.status_code == 401, missing.text
    # No user enumeration: same body as wrong-password.
    assert missing.json() == {"detail": "invalid credentials"}


def test_short_password_422(client: TestClient) -> None:
    short = client.post(
        "/auth/register",
        json={"email": "short@example.com", "password": "tiny1234"},
    )
    assert short.status_code == 422, short.text


def test_common_password_422(client: TestClient) -> None:
    common = client.post(
        "/auth/register",
        json={"email": "common@example.com", "password": "password123"},
    )
    assert common.status_code == 422, common.text
    assert common.json()["detail"] == "password is too common"


# ---------------------------------------------------------------------------
# Injection email is a literal string (parameterized queries only)
# ---------------------------------------------------------------------------


def test_injection_email_stored_literally(
    client: TestClient, store: MemoryStore
) -> None:
    reg = client.post(
        "/auth/register",
        json={"email": INJECTION_EMAIL, "password": "injection-proof-1"},
    )
    assert reg.status_code == 201, reg.text

    record = store.get_user_by_email(INJECTION_EMAIL)
    assert record is not None
    assert record.email == INJECTION_EMAIL  # literal, not executed

    # The injected "user" logs in normally and only sees itself.
    login = client.post(
        "/auth/login",
        json={"email": INJECTION_EMAIL, "password": "injection-proof-1"},
    )
    assert login.status_code == 200, login.text
    me = client.get(
        "/auth/me",
        headers={"Authorization": f"Bearer {login.json()['access_token']}"},
    )
    assert me.json()["email"] == INJECTION_EMAIL

    # And the tautology did NOT match any other row: unknown emails still 401.
    assert client.post(
        "/auth/login",
        json={"email": "' OR '1'='1", "password": "whatever-pass-1"},
    ).status_code == 401


# ---------------------------------------------------------------------------
# Token edge cases: missing / tampered / expired -> 401
# ---------------------------------------------------------------------------


def test_me_without_token_401(client: TestClient) -> None:
    assert client.get("/auth/me").status_code == 401


def test_me_tampered_token_401(
    client: TestClient, registered: dict
) -> None:
    login = client.post(
        "/auth/login",
        json={"email": registered["email"],
              "password": registered["password"]},
    )
    token = login.json()["access_token"]
    tampered = token[:-1] + ("a" if token[-1] != "a" else "b")
    resp = client.get(
        "/auth/me", headers={"Authorization": f"Bearer {tampered}"})
    assert resp.status_code == 401, resp.text


def test_me_expired_token_401(client: TestClient, registered: dict) -> None:
    import datetime

    from identity import auth as auth_module

    secret = auth_module.get_jwt_secret()
    expired = pyjwt.encode(
        {
            "sub": registered["user_id"],
            "ws": None,
            "exp": datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(seconds=10),
            "iat": datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(hours=2),
            "type": "access",
        },
        secret,
        algorithm="HS256",
    )
    resp = client.get(
        "/auth/me", headers={"Authorization": f"Bearer {expired}"})
    assert resp.status_code == 401, resp.text
    assert resp.json() == {"detail": "token expired"}


def test_rapid_wrong_logins_no_crash(client: TestClient) -> None:
    """20 rapid wrong-password logins: all 401, service stays up.

    Rate limiting is DOCUMENTED as deferred (README), not silently absent —
    this pins the no-crash half of that contract.
    """
    for _ in range(20):
        resp = client.post(
            "/auth/login",
            json={"email": "nobody@example.com",
                  "password": "wrong-password-1"},
        )
        assert resp.status_code == 401
    assert client.get("/health").status_code == 200


# ---------------------------------------------------------------------------
# Prod fail-fast: OMNIWATCH_ENV=prod without a secret refuses to boot
# ---------------------------------------------------------------------------


def test_prod_without_secret_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OMNIWATCH_ENV", "prod")
    monkeypatch.delenv("OMNIWATCH_JWT_SECRET", raising=False)
    monkeypatch.delenv("JWT_SECRET", raising=False)
    with pytest.raises(RuntimeError, match="OMNIWATCH_JWT_SECRET"):
        create_app(store=MemoryStore(), settings=None)


def test_prod_with_secret_boots(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OMNIWATCH_ENV", "prod")
    monkeypatch.setenv("OMNIWATCH_JWT_SECRET", "prod-secret-injected-by-env")
    app = create_app(store=MemoryStore(), settings=None)
    client = TestClient(app)
    assert client.get("/health").status_code == 200


def test_blocklist_has_minimum_entries() -> None:
    from identity import auth as auth_module

    assert len(auth_module.load_blocklist()) >= 50
