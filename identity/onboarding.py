"""
OmniWatch — Entry-Point / Workspace Layer
Component: Onboarding wizard API (app details + capacity capture)
Phase: entry-point (Wave 2, todo 3)
Purpose: POST + GET /workspaces/{id}/onboarding — JWT-required owner-only
         wizard capturing the FUP capacity question set; deterministic
         suggested_config (advisory only, never applied); persists to the
         workspaces row mirror AND MinIO workspaces/<slug>/config.json
Inputs: OnboardingSubmit JSON (identity/models.py), Bearer access JWT
Outputs: OnboardingResponse (answers + suggested_config + config_path);
         401 unauthenticated, 403 non-owner/unknown, 422 enum/range/injection
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status

from identity.auth import AuthContext, require_user
from identity.models import (
    OnboardingResponse,
    OnboardingSubmit,
    SuggestedConfig,
    VolumeBand,
)

logger = logging.getLogger("omniwatch.identity.onboarding")

router = APIRouter(prefix="/workspaces", tags=["onboarding"])

# ---------------------------------------------------------------------------
# Deterministic capacity mapping (pure function of bands only)
# ---------------------------------------------------------------------------

# Band severity order (shared index for expected_eps + log_volume).
_BAND_LEVEL: dict[str, int] = {"<100": 0, "100-1k": 1, "1k-10k": 2, ">10k": 3}

# Per-level tunables: higher bands -> larger queue/batch, tighter polls.
# Values mirror the FUP capacity discussion (advisory only, nothing applied).
_SUGGESTIONS: tuple[dict[str, int], ...] = (
    {"queue_depth": 500, "batch_size": 50,
     "poll_interval_s": 60, "scrape_interval_s": 60},
    {"queue_depth": 2000, "batch_size": 200,
     "poll_interval_s": 30, "scrape_interval_s": 30},
    {"queue_depth": 8000, "batch_size": 500,
     "poll_interval_s": 15, "scrape_interval_s": 15},
    {"queue_depth": 20000, "batch_size": 1000,
     "poll_interval_s": 5, "scrape_interval_s": 10},
)


def suggest_config(expected_eps: VolumeBand, log_volume: VolumeBand) -> SuggestedConfig:
    """Map capacity bands to agent tunables (deterministic, no I/O).

    Level = max(eps band, log band) so the heavier direction sizes the
    suggestion. Same bands in -> byte-identical suggestions out (no
    randomness, no live measurement).
    """
    level = max(_BAND_LEVEL[expected_eps], _BAND_LEVEL[log_volume])
    return SuggestedConfig(**_SUGGESTIONS[level])


# ---------------------------------------------------------------------------
# In-memory onboarding store (dev/tests backend; CH + MinIO mirror best-effort)
# ---------------------------------------------------------------------------

_LOCK = threading.Lock()
_ONBOARDING: dict[str, dict[str, Any]] = {}
# Write-through cache of every config.json payload (MinIO body proof even
# when no MinIO endpoint is reachable; keyed by full bucket/key path).
_CONFIG_JSON_CACHE: dict[str, str] = {}


def reset_onboarding_store() -> None:
    """Clear onboarding state (tests only; called per app build)."""
    with _LOCK:
        _ONBOARDING.clear()
        _CONFIG_JSON_CACHE.clear()


def get_cached_config_json() -> dict[str, str]:
    """Return the config.json write-through cache (tests/evidence only)."""
    with _LOCK:
        return dict(_CONFIG_JSON_CACHE)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _config_bucket() -> str:
    """Config bucket: env override, else the incidents bucket (no new buckets)."""
    override = os.getenv("OMNIWATCH_MINIO_CONFIG_BUCKET", "").strip()
    if override:
        return override
    try:
        from storage.config import MINIO_BUCKETS

        return MINIO_BUCKETS.get("incidents", "omniwatch-incidents")
    except Exception:  # noqa: BLE001 - identity image ships without storage/
        return "omniwatch-incidents"


def _config_key(slug: str) -> str:
    """MinIO key for a workspace config (isolation prefix + config.json)."""
    try:
        from storage.config import workspace_prefix

        return f"{workspace_prefix(slug)}config.json"
    except Exception:  # noqa: BLE001 - fallback mirrors the doc formula
        prefix = "" if slug == "default" else f"workspaces/{slug}/"
        return f"{prefix}config.json"


def _persist_minio(bucket: str, key: str, payload: dict[str, Any]) -> str:
    """Write config.json to MinIO (best-effort; degraded never fails submit)."""
    body = json.dumps(payload, indent=2, sort_keys=True)
    with _LOCK:
        _CONFIG_JSON_CACHE[f"{bucket}/{key}"] = body
    try:
        from minio import Minio

        import io

        endpoint = os.getenv(
            "OMNIWATCH_MINIO_ENDPOINT",
            os.getenv("MINIO_ENDPOINT", "localhost:9010"),
        )
        access = os.getenv(
            "OMNIWATCH_MINIO_ACCESS_KEY",
            os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        )
        secret = os.getenv(
            "OMNIWATCH_MINIO_SECRET_KEY",
            os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        )
        secure = os.getenv(
            "OMNIWATCH_MINIO_SECURE", os.getenv("MINIO_SECURE", "false")
        ).lower() in ("1", "true", "yes", "on")
        client = Minio(endpoint, access_key=access,
                       secret_key=secret, secure=secure)
        data = body.encode("utf-8")
        if not client.bucket_exists(bucket):
            return "skipped (bucket missing)"
        client.put_object(bucket, key, io.BytesIO(data), length=len(data),
                          content_type="application/json")
        return "ok"
    except Exception as exc:  # noqa: BLE001 - degraded, never fatal
        logger.warning("onboarding minio persist degraded %s/%s: %s",
                       bucket, key, exc)
        return f"degraded: {type(exc).__name__}"


def _mirror_row_onboarding(workspace_id: str, user_id: str, slug: str,
                           answers: OnboardingSubmit,
                           config_json: str) -> str:
    """Mirror onboarding columns into CH `workspaces` (best-effort)."""
    store_mode = os.getenv(
        "OMNIWATCH_IDENTITY_STORE",
        os.getenv("IDENTITY_STORE", "memory")).strip().lower()
    if store_mode != "clickhouse":
        return "skipped (memory store)"
    try:
        import clickhouse_connect

        host = os.getenv("OMNIWATCH_CLICKHOUSE_HOST",
                         os.getenv("CLICKHOUSE_HOST", "localhost"))
        port = int(os.getenv("OMNIWATCH_CLICKHOUSE_HTTP_PORT",
                             os.getenv("CLICKHOUSE_PORT", "8123")))
        database = os.getenv("OMNIWATCH_CLICKHOUSE_DB",
                             os.getenv("CLICKHOUSE_DB", "omniwatch"))
        user = os.getenv("OMNIWATCH_CLICKHOUSE_USER",
                         os.getenv("CLICKHOUSE_USER", "default"))
        password = os.getenv("OMNIWATCH_CLICKHOUSE_PASSWORD",
                             os.getenv("CLICKHOUSE_PASSWORD", ""))
        client = clickhouse_connect.get_client(
            host=host, port=port, database=database, username=user,
            password=password, connect_timeout=2, send_receive_timeout=5,
        )
        try:
            client.insert(
                "workspaces",
                [[
                    workspace_id, user_id, answers.app_name, slug,
                    answers.app_type, answers.cloud_provider,
                    list(answers.service_endpoints), answers.expected_eps,
                    answers.retention_days,
                    datetime.now(timezone.utc).replace(tzinfo=None), 0,
                    answers.app_name, answers.app_type,
                    answers.cloud_provider,
                    list(answers.service_endpoints),
                    answers.expected_eps, answers.log_volume,
                    answers.retention_days, answers.alert_contact,
                    config_json,
                    datetime.now(timezone.utc).replace(tzinfo=None),
                ]],
                column_names=[
                    "workspace_id", "user_id", "name", "slug", "app_type",
                    "cloud_provider", "endpoints", "expected_volume",
                    "retention_days", "created_at", "deleted",
                    "onboarding_app_name", "onboarding_app_type",
                    "onboarding_cloud_provider", "onboarding_endpoints",
                    "onboarding_expected_eps", "onboarding_log_volume",
                    "onboarding_retention_days", "onboarding_alert_contact",
                    "onboarding_config_json", "onboarding_updated_at",
                ],
            )
        finally:
            client.close()
        return "ok"
    except Exception as exc:  # noqa: BLE001 - degraded, never fatal
        logger.warning("onboarding row mirror degraded slug=%s: %s", slug, exc)
        return f"degraded: {type(exc).__name__}"


def _forbidden() -> HTTPException:
    """403 for unknown ids and cross-owner access (never 404-leak, never 200)."""
    return HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="workspace not found or access denied",
    )


def _build_response(workspace_id: str, slug: str,
                    answers: OnboardingSubmit) -> dict[str, Any]:
    """Assemble the persisted payload (POST and GET share this shape)."""
    suggestion = suggest_config(answers.expected_eps, answers.log_volume)
    bucket = _config_bucket()
    key = _config_key(slug)
    return {
        "workspace_id": workspace_id,
        "slug": slug,
        "answers": answers.model_dump(),
        "suggested_config": suggestion.model_dump(),
        "config_path": f"{bucket}/{key}",
        "updated_at": _utcnow_iso(),
    }


@router.post("/{workspace_id}/onboarding", response_model=OnboardingResponse)
def submit_onboarding(
    workspace_id: str,
    body: OnboardingSubmit,
    ctx: AuthContext = Depends(require_user),
) -> OnboardingResponse:
    """Submit (or resubmit) the wizard: validate -> store -> CH + MinIO."""
    from identity.workspaces import get_registry

    record = get_registry().get_owned(workspace_id, ctx.user_id)
    if record is None:
        raise _forbidden()
    payload = _build_response(workspace_id, record.slug, body)
    bucket = _config_bucket()
    key = _config_key(record.slug)
    minio_status = _persist_minio(bucket, key, payload)
    ch_status = _mirror_row_onboarding(
        workspace_id, ctx.user_id, record.slug, body,
        json.dumps(payload, indent=2, sort_keys=True),
    )
    with _LOCK:
        _ONBOARDING[workspace_id] = payload
    logger.info("onboarding submit slug=%s user_id=%s minio=%s ch=%s",
                record.slug, ctx.user_id, minio_status, ch_status)
    return OnboardingResponse(**payload)


@router.get("/{workspace_id}/onboarding", response_model=OnboardingResponse)
def read_onboarding(
    workspace_id: str,
    ctx: AuthContext = Depends(require_user),
) -> OnboardingResponse:
    """Re-read saved wizard answers + suggestions (idempotent; 404 pre-submit)."""
    from identity.workspaces import get_registry

    record = get_registry().get_owned(workspace_id, ctx.user_id)
    if record is None:
        raise _forbidden()
    with _LOCK:
        payload: Optional[dict[str, Any]] = _ONBOARDING.get(workspace_id)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="onboarding not completed for this workspace",
        )
    return OnboardingResponse(**payload)
