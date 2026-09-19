"""
OmniWatch — Entry-Point / Identity Layer
Component: Auth routers + require_user middleware dependency
Phase: entry-point (Wave 1)
Purpose: POST /auth/register, POST /auth/login, POST /auth/refresh,
         POST /auth/logout, GET /auth/me; bcrypt verification; refresh
         rotation with revocation; STABLE import path for later todos:
         ``from identity.auth import require_user``
Inputs: RegisterRequest/LoginRequest/RefreshRequest bodies, Bearer JWTs
Outputs: TokenResponse / RegisterResponse / MeResponse; 401/404/409/422 errors
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Optional

import bcrypt
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from identity.models import (
    LoginRequest,
    MeResponse,
    RefreshRequest,
    RegisterRequest,
    RegisterResponse,
    TokenResponse,
)
from identity.settings import IdentitySettings, resolve_jwt_secret
from identity.store import (
    ClickHouseStore,
    DuplicateEmailError,
    MemoryStore,
    utcnow,
)
from identity.tokens import (
    ExpiredTokenError,
    InvalidTokenError,
    create_access_token,
    create_refresh_token,
    decode_token,
    hash_refresh_token,
)

logger = logging.getLogger("omniwatch.identity.auth")

router = APIRouter(prefix="/auth", tags=["auth"])

_bearer = HTTPBearer(auto_error=False)

# Module-level wiring, set by configure() (main.create_app calls it; tests
# call it per-fixture for isolation). Kept module-level (not app.state) so
# the ``require_user`` import path stays a plain function import.
_STORE: MemoryStore | ClickHouseStore | None = None
_SETTINGS: IdentitySettings | None = None
_JWT_SECRET: str | None = None


def configure(
    store: MemoryStore | ClickHouseStore, settings: IdentitySettings
) -> None:
    """Wire store + settings + resolved secret (boot/test entry point)."""
    global _STORE, _SETTINGS, _JWT_SECRET
    _STORE = store
    _SETTINGS = settings
    _JWT_SECRET = resolve_jwt_secret(settings)


def get_store() -> MemoryStore | ClickHouseStore:
    """Return the configured store (500 if configure() was never called)."""
    if _STORE is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="identity store not configured",
        )
    return _STORE


def get_settings() -> IdentitySettings:
    """Return the configured settings (500 if configure() never called)."""
    if _SETTINGS is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="identity settings not configured",
        )
    return _SETTINGS


def get_jwt_secret() -> str:
    """Return the resolved JWT secret (500 if configure() never called)."""
    if _JWT_SECRET is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="identity JWT secret not configured",
        )
    return _JWT_SECRET


# ---------------------------------------------------------------------------
# Common-password blocklist (checked at register -> 422)
# ---------------------------------------------------------------------------

_BLOCKLIST_PATH = Path(__file__).resolve().parent / "common_passwords.txt"


def load_blocklist() -> set[str]:
    """Load lowercase blocklist entries (one per line, # comments)."""
    entries: set[str] = set()
    try:
        for line in _BLOCKLIST_PATH.read_text(encoding="utf-8").splitlines():
            line = line.strip().lower()
            if line and not line.startswith("#"):
                entries.add(line)
    except FileNotFoundError:
        logger.warning(
            "common-password blocklist missing at %s; register check degraded",
            _BLOCKLIST_PATH,
        )
    return entries


_BLOCKLIST = load_blocklist()


def is_blocked_password(password: str) -> bool:
    """True when the password is on the common-password blocklist."""
    return password.strip().lower() in _BLOCKLIST


# ---------------------------------------------------------------------------
# Password hashing (bcrypt only — never plaintext, never logged)
# ---------------------------------------------------------------------------


def hash_password(password: str) -> str:
    """bcrypt-hash a password (returns the $2b$ hash string)."""
    return bcrypt.hashpw(
        password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, pw_hash: str) -> bool:
    """Constant-time bcrypt verification; False on any mismatch/error."""
    try:
        return bcrypt.checkpw(
            password.encode("utf-8"), pw_hash.encode("utf-8"))
    except Exception:  # noqa: BLE001 - malformed hash must fail closed
        return False


# ---------------------------------------------------------------------------
# Auth context + require_user (STABLE import path for todos 2-5)
# ---------------------------------------------------------------------------


@dataclass
class AuthContext:
    """Authenticated caller: user identity + active workspace (ws claim).

    ``workspace_id`` is None until todo-2 workspace switch re-issues the JWT
    with a ``ws`` claim — workspace routers must treat None as unauthenticated
    for scoped routes (or map to the default workspace per the plan).
    """

    user_id: str
    email: str
    workspace_id: Optional[str]
    claims: dict[str, Any]


async def require_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer),
) -> AuthContext:
    """FastAPI dependency enforcing JWT auth (missing/invalid/expired -> 401).

    Stable import path (do NOT move without updating todos 2-5)::

        from identity.auth import require_user
    """
    if credentials is None or not credentials.credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing credentials",
        )
    try:
        claims = decode_token(credentials.credentials, get_jwt_secret())
    except ExpiredTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="token expired",
        ) from exc
    except InvalidTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid token",
        ) from exc
    if claims.get("type", "access") != "access":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid token",
        )
    user = get_store().get_user_by_id(str(claims.get("sub", "")))
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid token",
        )
    return AuthContext(
        user_id=user.user_id,
        email=user.email,
        workspace_id=claims.get("ws"),
        claims=claims,
    )


# ---------------------------------------------------------------------------
# Token-pair issuance (login + refresh share this rotation core)
# ---------------------------------------------------------------------------


def _issue_pair(user_id: str, ws: Optional[str] = None) -> TokenResponse:
    """Mint access + refresh; persist the refresh hash as a live session."""
    import uuid as _uuid

    settings = get_settings()
    secret = get_jwt_secret()
    access = create_access_token(user_id, secret, settings.jwt_ttl_s, ws)
    session_id = str(_uuid.uuid4())
    refresh = create_refresh_token(
        user_id, secret, settings.jwt_refresh_ttl_s, session_id)
    get_store().create_session(
        user_id,
        refresh_hash=hash_refresh_token(refresh),
        expires_at=utcnow() + timedelta(seconds=settings.jwt_refresh_ttl_s),
        session_id=session_id,
    )
    return TokenResponse(
        access_token=access,
        refresh_token=refresh,
        expires_in=settings.jwt_ttl_s,
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.post(
    "/register", response_model=RegisterResponse,
    status_code=status.HTTP_201_CREATED)
def register(body: RegisterRequest) -> RegisterResponse:
    """Register a new user (bcrypt hash -> users row)."""
    email = body.email.strip()
    if not email:
        raise HTTPException(
            status_code=422,  # raw int: starlette renamed
            # HTTP_422_UNPROCESSABLE_ENTITY; the int is version-proof
            detail="email must not be empty",
        )
    if is_blocked_password(body.password):
        raise HTTPException(
            status_code=422,
            detail="password is too common",
        )
    try:
        record = get_store().create_user(email, hash_password(body.password))
    except DuplicateEmailError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="email already registered",
        ) from exc
    logger.info("user registered user_id=%s", record.user_id)
    return RegisterResponse(
        user_id=record.user_id,
        email=record.email,
        created_at=record.created_at.isoformat(),
    )


@router.post("/login", response_model=TokenResponse)
def login(body: LoginRequest) -> TokenResponse:
    """Verify credentials -> short-lived access JWT + longer refresh JWT."""
    user = get_store().get_user_by_email(body.email.strip())
    if user is None or not verify_password(body.password, user.pw_hash):
        # Identical response for unknown email vs wrong password (no oracle).
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid credentials",
        )
    logger.info("user login user_id=%s", user.user_id)
    return _issue_pair(user.user_id)


@router.post("/refresh", response_model=TokenResponse)
def refresh(body: RefreshRequest) -> TokenResponse:
    """Rotate a refresh token: old hash revoked, brand-new pair issued.

    Replay of an already-used (revoked) refresh token -> 401.
    """
    try:
        claims = decode_token(body.refresh_token, get_jwt_secret())
    except ExpiredTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="refresh token expired",
        ) from exc
    except InvalidTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid refresh token",
        ) from exc
    if claims.get("type") != "refresh":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid refresh token",
        )
    incoming_hash = hash_refresh_token(body.refresh_token)
    session = get_store().get_session_by_refresh_hash(incoming_hash)
    if session is None or session.revoked:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid refresh token",
        )
    if session.expires_at.tzinfo is None:
        expiry = session.expires_at.replace(tzinfo=utcnow().tzinfo)
    else:
        expiry = session.expires_at
    if expiry <= utcnow():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="refresh token expired",
        )
    # Rotation: revoke the presented refresh BEFORE issuing the new pair so
    # a replayed (already-rotated) token is rejected above.
    get_store().revoke_session_by_hash(incoming_hash)
    logger.info("refresh rotation user_id=%s", session.user_id)
    return _issue_pair(session.user_id, claims.get("ws"))


@router.post("/logout")
def logout(body: RefreshRequest) -> dict[str, str]:
    """Revoke a refresh token (idempotent — unknown tokens still 200)."""
    get_store().revoke_session_by_hash(
        hash_refresh_token(body.refresh_token))
    return {"status": "logged out"}


@router.get("/me", response_model=MeResponse)
def me(ctx: AuthContext = Depends(require_user)) -> MeResponse:
    """Return the JWT holder's profile (JWT-required)."""
    user = get_store().get_user_by_id(ctx.user_id)
    if user is None:  # user deleted after token issuance
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid token",
        )
    created = user.created_at
    created_at = (
        created.isoformat() if hasattr(created, "isoformat") else str(created)
    )
    return MeResponse(
        user_id=user.user_id, email=user.email, created_at=created_at)
