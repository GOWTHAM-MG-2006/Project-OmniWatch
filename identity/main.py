"""
OmniWatch — Entry-Point / Identity Layer
Component: Identity Service (FastAPI app)
Phase: entry-point (Wave 1)
Purpose: FastAPI application for register/login/JWT auth on port 8012 with
         GET /health; mirrors learning/learning_service.py skeleton
         (create_app factory + module app + uvicorn main)
Inputs: HTTP /auth/* + /workspaces* + /health; OMNIWATCH_* env
         (JWT secret fail-fast in prod)
Outputs: GET /health, POST /auth/register|login|refresh|logout, GET /auth/me,
         POST|GET /workspaces, GET|PATCH|DELETE /workspaces/{id},
         POST /workspaces/{id}/switch
"""

from __future__ import annotations

import logging
import os
import sys

from fastapi import FastAPI

from identity.auth import configure, router as auth_router
from identity.onboarding import reset_onboarding_store, router as onboarding_router
from identity.settings import IdentitySettings
from identity.store import ClickHouseStore, MemoryStore, build_store
from identity.workspaces import reset_registry, router as workspaces_router

logger = logging.getLogger("omniwatch.identity.service")


def _L(primary: str, fallback: str, default: str) -> str:
    """OMNIWATCH_* primary with old bare-name fallback (backwards compat)."""
    return os.environ.get(primary, os.environ.get(fallback, default))


IDENTITY_PORT = int(_L("OMNIWATCH_IDENTITY_PORT", "IDENTITY_PORT", "8012"))


def create_app(
    store: MemoryStore | ClickHouseStore | None = None,
    settings: IdentitySettings | None = None,
) -> FastAPI:
    """Build the identity FastAPI application.

    Args:
        store: store backend (fresh MemoryStore per test when given).
        settings: explicit settings (fresh from env when None).

    Raises:
        RuntimeError: OMNIWATCH_ENV=prod without OMNIWATCH_JWT_SECRET
            (fail-fast at boot via configure -> resolve_jwt_secret).
    """
    cfg = settings or IdentitySettings.from_env()
    active_store = store if store is not None else build_store(cfg)
    configure(active_store, cfg)  # fail-fasts in prod without a secret

    application = FastAPI(
        title="OmniWatch Identity Service",
        version="1.0.0",
    )

    @application.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "identity"}

    application.include_router(auth_router)
    reset_registry()  # fresh workspace state per app build (test isolation)
    reset_onboarding_store()  # fresh wizard state per app build (ENTRY-3)
    application.include_router(workspaces_router)
    application.include_router(onboarding_router)
    logger.info("identity service configured port=%d", cfg.identity_port)
    return application


app = create_app()


def main() -> None:
    """Run the identity service via uvicorn."""
    import uvicorn

    uvicorn.run(
        "identity.main:app",
        host="0.0.0.0",
        port=IDENTITY_PORT,
        reload=False,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    main()
