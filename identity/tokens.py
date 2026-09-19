"""
OmniWatch — Entry-Point / Identity Layer
Component: JWT helpers (access + refresh issuance/verification)
Phase: entry-point (Wave 1)
Purpose: Mint and verify HS256 JWTs with claims sub=user_id, ws (active
         workspace, None until todo-2 switch), exp (+iat, type, sid on refresh)
Inputs: user_id, secret (env-only via settings.resolve_jwt_secret), TTLs
Outputs: Encoded tokens / decoded payloads; InvalidTokenError/ExpiredTokenError
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import jwt

ALGORITHM = "HS256"


class InvalidTokenError(Exception):
    """Token is malformed, tampered, or signed with the wrong secret (401)."""


class ExpiredTokenError(InvalidTokenError):
    """Token signature is valid but exp has passed (401)."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def create_access_token(
    user_id: str,
    secret: str,
    ttl_s: int,
    workspace_id: Optional[str] = None,
) -> str:
    """Mint a short-lived access JWT (claims: sub, ws, exp, iat, type)."""
    now = _now()
    payload = {
        "sub": user_id,
        "ws": workspace_id,
        "exp": now + timedelta(seconds=ttl_s),
        "iat": now,
        "type": "access",
    }
    return jwt.encode(payload, secret, algorithm=ALGORITHM)


def create_refresh_token(
    user_id: str, secret: str, refresh_ttl_s: int, session_id: str
) -> str:
    """Mint a longer-lived refresh JWT bound to a session row (sid claim)."""
    now = _now()
    payload = {
        "sub": user_id,
        "sid": session_id or str(uuid.uuid4()),
        "exp": now + timedelta(seconds=refresh_ttl_s),
        "iat": now,
        "type": "refresh",
    }
    return jwt.encode(payload, secret, algorithm=ALGORITHM)


def decode_token(token: str, secret: str) -> dict[str, Any]:
    """Verify signature + expiry and return claims.

    Raises:
        ExpiredTokenError: signature ok, token expired.
        InvalidTokenError: malformed/tampered/wrong-secret/alg mismatch.
    """
    try:
        payload: dict[str, Any] = jwt.decode(
            token,
            secret,
            algorithms=[ALGORITHM],
            options={"require": ["exp", "sub"]},
        )
        return payload
    except jwt.ExpiredSignatureError as exc:
        raise ExpiredTokenError("token expired") from exc
    except jwt.InvalidTokenError as exc:
        raise InvalidTokenError(f"invalid token: {exc}") from exc


def hash_refresh_token(refresh_token: str) -> str:
    """SHA-256 hex of a refresh token (what the sessions table stores).

    Raw refresh tokens are never persisted — only this hash — so a DB read
    alone cannot mint new access tokens.
    """
    return hashlib.sha256(refresh_token.encode("utf-8")).hexdigest()
