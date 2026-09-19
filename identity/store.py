"""
OmniWatch — Entry-Point / Identity Layer
Component: User/session store (Memory + ClickHouse backends)
Phase: entry-point (Wave 1)
Purpose: Persistence for users + refresh sessions behind one interface.
         MemoryStore is the default (dev/tests); ClickHouseStore targets the
         migration-002 tables. ALL ClickHouse queries are parameterized —
         user input is never interpolated into SQL.
Inputs: UserRecord / SessionRecord fields (email, bcrypt hash, token hashes)
Outputs: Stored/queried records; DuplicateEmailError on email collision
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from identity.settings import IdentitySettings


class DuplicateEmailError(Exception):
    """Raised when registering an email that already exists (maps to 409)."""


def utcnow() -> datetime:
    """Timezone-aware UTC now (ClickHouse DateTime compatible)."""
    return datetime.now(timezone.utc)


def new_id() -> str:
    """Random UUID string for user_id / session_id."""
    return str(uuid.uuid4())


@dataclass
class UserRecord:
    """One row of the `users` table."""

    user_id: str
    email: str
    pw_hash: str
    created_at: datetime


@dataclass
class SessionRecord:
    """One row of the `sessions` table (refresh-token tracking)."""

    session_id: str
    user_id: str
    refresh_hash: str
    expires_at: datetime
    revoked: bool = False


class MemoryStore:
    """Thread-safe in-memory store (dev default + test backend).

    Email uniqueness is enforced here, mirroring the ClickHouse backend's
    SELECT-before-INSERT check (ClickHouse has no UNIQUE constraint).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._by_id: dict[str, UserRecord] = {}
        self._by_email: dict[str, UserRecord] = {}
        self._sessions: dict[str, SessionRecord] = {}

    def reset(self) -> None:
        """Clear all state (tests only)."""
        with self._lock:
            self._by_id.clear()
            self._by_email.clear()
            self._sessions.clear()

    # -- users -------------------------------------------------------- #
    def create_user(self, email: str, pw_hash: str) -> UserRecord:
        with self._lock:
            if email in self._by_email:
                raise DuplicateEmailError(email)
            record = UserRecord(
                user_id=new_id(),
                email=email,
                pw_hash=pw_hash,
                created_at=utcnow(),
            )
            self._by_id[record.user_id] = record
            self._by_email[email] = record
            return record

    def get_user_by_email(self, email: str) -> Optional[UserRecord]:
        with self._lock:
            return self._by_email.get(email)

    def get_user_by_id(self, user_id: str) -> Optional[UserRecord]:
        with self._lock:
            return self._by_id.get(user_id)

    # -- sessions ----------------------------------------------------- #
    def create_session(
        self, user_id: str, refresh_hash: str, expires_at: datetime,
        session_id: str | None = None,
    ) -> SessionRecord:
        with self._lock:
            record = SessionRecord(
                session_id=session_id or new_id(),
                user_id=user_id,
                refresh_hash=refresh_hash,
                expires_at=expires_at,
                revoked=False,
            )
            self._sessions[refresh_hash] = record
            return record

    def get_session_by_refresh_hash(
        self, refresh_hash: str
    ) -> Optional[SessionRecord]:
        with self._lock:
            return self._sessions.get(refresh_hash)

    def revoke_session_by_hash(self, refresh_hash: str) -> bool:
        """Mark a refresh session revoked. Returns True if one was found."""
        with self._lock:
            record = self._sessions.get(refresh_hash)
            if record is None:
                return False
            record.revoked = True
            return True


class ClickHouseStore:
    """ClickHouse-backed store over the migration-002 tables.

    Every statement is parameterized (``%(name)s`` + ``parameters=``) so
    hostile input such as ``' OR 1=1--`` is bound as a literal value and can
    never alter query structure. Email uniqueness is enforced with an
    explicit SELECT-before-INSERT (ClickHouse has no UNIQUE constraint).
    """

    def __init__(self, cfg: IdentitySettings) -> None:
        import clickhouse_connect  # lazy: keeps unit-test import light

        self._client = clickhouse_connect.get_client(
            host=cfg.clickhouse_host,
            port=cfg.clickhouse_port,
            database=cfg.clickhouse_db,
            username=cfg.clickhouse_user,
            password=cfg.clickhouse_password,
        )

    def close(self) -> None:
        """Release the ClickHouse client."""
        try:
            self._client.close()
        except Exception:  # noqa: BLE001 - best-effort close at shutdown
            pass

    # -- users -------------------------------------------------------- #
    def create_user(self, email: str, pw_hash: str) -> UserRecord:
        existing = self._client.query(
            "SELECT user_id FROM users WHERE email = %(email)s LIMIT 1",
            parameters={"email": email},
        )
        if existing.result_rows:
            raise DuplicateEmailError(email)
        record = UserRecord(
            user_id=new_id(),
            email=email,
            pw_hash=pw_hash,
            created_at=utcnow(),
        )
        self._client.insert(
            "users",
            [[record.user_id, record.email, record.pw_hash,
              record.created_at.replace(tzinfo=None)]],
            column_names=["user_id", "email", "pw_hash", "created_at"],
        )
        return record

    def get_user_by_email(self, email: str) -> Optional[UserRecord]:
        result = self._client.query(
            "SELECT user_id, email, pw_hash, created_at FROM users "
            "WHERE email = %(email)s LIMIT 1",
            parameters={"email": email},
        )
        if not result.result_rows:
            return None
        row = result.result_rows[0]
        return UserRecord(
            user_id=str(row[0]), email=row[1], pw_hash=row[2],
            created_at=row[3],
        )

    def get_user_by_id(self, user_id: str) -> Optional[UserRecord]:
        result = self._client.query(
            "SELECT user_id, email, pw_hash, created_at FROM users "
            "WHERE user_id = %(user_id)s LIMIT 1",
            parameters={"user_id": user_id},
        )
        if not result.result_rows:
            return None
        row = result.result_rows[0]
        return UserRecord(
            user_id=str(row[0]), email=row[1], pw_hash=row[2],
            created_at=row[3],
        )

    # -- sessions ----------------------------------------------------- #
    def create_session(
        self, user_id: str, refresh_hash: str, expires_at: datetime,
        session_id: str | None = None,
    ) -> SessionRecord:
        record = SessionRecord(
            session_id=session_id or new_id(),
            user_id=user_id,
            refresh_hash=refresh_hash,
            expires_at=expires_at.replace(tzinfo=None),
            revoked=False,
        )
        self._client.insert(
            "sessions",
            [[record.session_id, record.user_id, record.refresh_hash,
              record.expires_at, 0]],
            column_names=[
                "session_id", "user_id", "refresh_hash",
                "expires_at", "revoked"],
        )
        record.expires_at = record.expires_at.replace(tzinfo=timezone.utc)
        return record

    def get_session_by_refresh_hash(
        self, refresh_hash: str
    ) -> Optional[SessionRecord]:
        result = self._client.query(
            "SELECT session_id, user_id, refresh_hash, expires_at, revoked "
            "FROM sessions WHERE refresh_hash = %(h)s LIMIT 1",
            parameters={"h": refresh_hash},
        )
        if not result.result_rows:
            return None
        row = result.result_rows[0]
        return SessionRecord(
            session_id=str(row[0]),
            user_id=str(row[1]),
            refresh_hash=row[2],
            expires_at=row[3],
            revoked=bool(row[4]),
        )

    def revoke_session_by_hash(self, refresh_hash: str) -> bool:
        result = self._client.query(
            "SELECT session_id FROM sessions "
            "WHERE refresh_hash = %(h)s LIMIT 1",
            parameters={"h": refresh_hash},
        )
        if not result.result_rows:
            return False
        self._client.command(
            "ALTER TABLE sessions UPDATE revoked = 1 "
            "WHERE refresh_hash = %(h)s",
            parameters={"h": refresh_hash},
        )
        return True


def build_store(cfg: IdentitySettings) -> MemoryStore | ClickHouseStore:
    """Select the store backend (``memory`` default; ``clickhouse`` live)."""
    if (cfg.identity_store or "memory").strip().lower() == "clickhouse":
        return ClickHouseStore(cfg)
    return MemoryStore()
