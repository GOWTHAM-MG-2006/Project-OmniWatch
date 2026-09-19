"""
OmniWatch — Entry-Point / Identity Layer
Component: Settings (env resolution + prod fail-fast)
Phase: entry-point (Wave 1)
Purpose: OMNIWATCH_* env config for the identity service; JWT secret is
         env-only and fail-fast when OMNIWATCH_ENV=prod
Inputs: OMNIWATCH_ENV, OMNIWATCH_JWT_SECRET, OMNIWATCH_JWT_TTL_S,
        OMNIWATCH_JWT_REFRESH_TTL_S, OMNIWATCH_IDENTITY_PORT,
        OMNIWATCH_IDENTITY_STORE, CLICKHOUSE_* (bare fallbacks honored)
Outputs: IdentitySettings object + resolve_jwt_secret() (prod gate)
"""

from __future__ import annotations

import logging
import os

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger("omniwatch.identity.settings")

# Dev-only fallback. NEVER committed as a real credential — prod boot refuses
# it outright (see resolve_jwt_secret). Loud WARN whenever it is in effect.
DEV_DEFAULT_JWT_SECRET = "omniwatch-dev-jwt-secret-NOT-FOR-PROD"


def _L(primary: str, fallback: str, default: str) -> str:
    """OMNIWATCH_* primary with old bare-name fallback (backwards compat)."""
    return os.environ.get(primary, os.environ.get(fallback, default))


class IdentitySettings(BaseSettings):
    """Configuration for the Identity service.

    Every field is overridable via its OMNIWATCH_* variable (primary) or the
    old bare name (deprecated fallback), following the DYN-python
    ``os.getenv("OMNIWATCH_X", os.getenv("OLD", "default"))`` idiom with
    ``AliasChoices`` in pydantic settings.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    app_env: str = Field(
        default="dev",
        validation_alias=AliasChoices("OMNIWATCH_ENV", "ENV"),
    )
    jwt_secret: str = Field(
        default=DEV_DEFAULT_JWT_SECRET,
        validation_alias=AliasChoices("OMNIWATCH_JWT_SECRET", "JWT_SECRET"),
    )
    jwt_ttl_s: int = Field(
        default=3600,
        validation_alias=AliasChoices("OMNIWATCH_JWT_TTL_S", "JWT_TTL_S"),
    )
    jwt_refresh_ttl_s: int = Field(
        default=86400,
        validation_alias=AliasChoices(
            "OMNIWATCH_JWT_REFRESH_TTL_S", "JWT_REFRESH_TTL_S"),
    )
    identity_port: int = Field(
        default=8012,
        validation_alias=AliasChoices(
            "OMNIWATCH_IDENTITY_PORT", "IDENTITY_PORT"),
    )
    identity_store: str = Field(
        default="memory",
        validation_alias=AliasChoices(
            "OMNIWATCH_IDENTITY_STORE", "IDENTITY_STORE"),
    )

    clickhouse_host: str = Field(
        default="localhost",
        validation_alias=AliasChoices(
            "OMNIWATCH_CLICKHOUSE_HOST", "CLICKHOUSE_HOST"))
    clickhouse_port: int = Field(
        default=8123,
        validation_alias=AliasChoices(
            "OMNIWATCH_CLICKHOUSE_HTTP_PORT", "CLICKHOUSE_PORT"))
    clickhouse_db: str = Field(
        default="omniwatch",
        validation_alias=AliasChoices(
            "OMNIWATCH_CLICKHOUSE_DB", "CLICKHOUSE_DB"))
    clickhouse_user: str = Field(
        default="default",
        validation_alias=AliasChoices(
            "OMNIWATCH_CLICKHOUSE_USER", "CLICKHOUSE_USER"))
    clickhouse_password: str = Field(
        default="",
        validation_alias=AliasChoices(
            "OMNIWATCH_CLICKHOUSE_PASSWORD", "CLICKHOUSE_PASSWORD"))

    @classmethod
    def from_env(cls) -> "IdentitySettings":
        """Build settings from environment variables (dev defaults)."""
        return cls()


def resolve_jwt_secret(cfg: IdentitySettings) -> str:
    """Return the effective JWT secret, enforcing the prod fail-fast gate.

    - ``OMNIWATCH_ENV=prod`` + missing/empty/dev-default secret ->
      ``RuntimeError`` naming ``OMNIWATCH_JWT_SECRET`` (fail fast at boot).
    - Otherwise the dev default is returned with a loud WARN.
    """
    env = (cfg.app_env or "dev").strip().lower()
    secret = (cfg.jwt_secret or "").strip()
    is_default = secret == "" or secret == DEV_DEFAULT_JWT_SECRET
    if env == "prod" and is_default:
        raise RuntimeError(
            "refusing to boot with dev-default JWT secret: set "
            "OMNIWATCH_JWT_SECRET to a non-default value via environment "
            "(OMNIWATCH_ENV=prod)"
        )
    if is_default:
        logger.warning(
            "WARN identity using dev-default JWT secret; set "
            "OMNIWATCH_JWT_SECRET in prod (OMNIWATCH_ENV=prod fail-fasts)"
        )
        return DEV_DEFAULT_JWT_SECRET
    return secret
