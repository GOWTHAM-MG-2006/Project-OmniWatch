# identity — Entry-Point Auth Service (Wave 1)

Password registration + login + JWT session auth, ClickHouse-backed.
Port **8012**. No OAuth/OIDC, no email sending, no new stateful infra.

## Endpoints

| Method | Path | Auth | Success | Errors |
|--------|------|------|---------|--------|
| GET | `/health` | no | 200 `{"status":"ok"}` | — |
| POST | `/auth/register` | no | 201 `{user_id,email,created_at}` | 409 duplicate, 422 short/common/empty |
| POST | `/auth/login` | no | 200 `{access_token,refresh_token,token_type,expires_in}` | 401 bad creds |
| POST | `/auth/refresh` | refresh JWT (body) | 200 new pair (rotation) | 401 revoked/expired/tampered |
| POST | `/auth/logout` | refresh JWT (body) | 200 (idempotent) | — |
| GET | `/auth/me` | Bearer access JWT | 200 `{user_id,email,created_at}` | 401 missing/tampered/expired |

JWT claims: `sub=user_id`, `ws` (active workspace; `None` until todo-2
switch re-issues), `exp` (+`iat`, `type`). Access TTL
`OMNIWATCH_JWT_TTL_S` (3600); refresh `OMNIWATCH_JWT_REFRESH_TTL_S` (86400).

## Middleware import (STABLE — todos 2-5 import this)

```python
from identity.auth import require_user  # -> AuthContext(user_id, email, workspace_id, claims)
```

## Security rules

- Passwords: bcrypt hashes only, never plaintext, never logged.
- JWT secret: env-only (`OMNIWATCH_JWT_SECRET`); prod boot fail-fasts
  without it; dev default logs a loud WARN.
- All ClickHouse queries parameterized — `' OR 1=1--` is a literal string.
- Refresh rotation: each use revokes the old hash; replay -> 401.
- Rate limiting is DEFERRED (documented, not silently absent): sustained
  wrong-password loops return 401 without crashing but are not throttled yet.

## Environment

| Var | Default | Notes |
|-----|---------|-------|
| `OMNIWATCH_ENV` | `dev` | `prod` enforces the JWT-secret gate |
| `OMNIWATCH_JWT_SECRET` | dev default + WARN | **no value committed** |
| `OMNIWATCH_JWT_TTL_S` | `3600` | access lifetime |
| `OMNIWATCH_JWT_REFRESH_TTL_S` | `86400` | refresh lifetime |
| `OMNIWATCH_IDENTITY_PORT` | `8012` | serve port |
| `OMNIWATCH_IDENTITY_STORE` | `memory` | `memory` (dev/tests) or `clickhouse` |
| `OMNIWATCH_CLICKHOUSE_*` | compose defaults | live store backend |

## Run

```bash
# dev (in-memory store)
python -m uvicorn identity.main:app --port 8012

# live ClickHouse store (after migration 002)
OMNIWATCH_IDENTITY_STORE=clickhouse python -m uvicorn identity.main:app --port 8012

# migration
python -m storage.clickhouse.migrations.002_identity

# tests
pytest identity/tests/ -v
```
