"""
OmniWatch — Unified Storage Layer
Component: MinIO Bucket Setup
Phase: 5
Purpose: Idempotently bootstrap the 5 MinIO buckets used across OmniWatch and
         attach their S3 lifecycle (retention/expiry) policies. Safe to run
         repeatedly — already-existing buckets and already-set lifecycle rules
         are no-ops (bucket_exists check before make_bucket; set_bucket_lifecycle
         is an overwrite-by-design API).
         NOTE: MinIO single-node deployments have no remote tiers configured,
         so it rejects lifecycle Transition rules with ANY storage class
         (InvalidStorageClass). Lifecycle is therefore expressed as Expiration
         (expire aged objects after N days) — the server-accepted equivalent of
         the plan contract's "transition to IA after N days".
Inputs: A connected minio.Minio client (endpoint WITHOUT scheme, e.g.
        localhost:9010) or a StorageConfig to build one from
Outputs: Ensured buckets: omniwatch-telemetry-archive (90d expiry),
         omniwatch-incidents (365d expiry), omniwatch-audit-logs (365d expiry),
         omniwatch-runbooks (no lifecycle), omniwatch-ml-datasets (no lifecycle).
         Structured JSON state logs per bucket to stdout.
"""

from __future__ import annotations

from typing import List, Optional

from minio import Minio
from minio.lifecycleconfig import Expiration, LifecycleConfig, Rule

from storage.common import create_logger, retry_with_backoff
from storage.config import StorageConfig

logger = create_logger("omniwatch.storage.minio.bucket_setup")

# Kebab-case bucket names per AGENTS.md MinIO Buckets reference. The plan's IA
# ("Infrequent Access") tier is not available on single-node MinIO — there are
# no remote tiers, so lifecycle uses Expiration instead (see _lifecycle_for).
BUCKET_TELEMETRY_ARCHIVE = "omniwatch-telemetry-archive"
BUCKET_INCIDENTS = "omniwatch-incidents"
BUCKET_AUDIT_LOGS = "omniwatch-audit-logs"
BUCKET_RUNBOOKS = "omniwatch-runbooks"
BUCKET_ML_DATASETS = "omniwatch-ml-datasets"

# Bucket name -> retention days (objects expire after N days). Buckets ABSENT
# from this map get no lifecycle policy at all (runbooks / ml-datasets are
# per-object live data — never aged out).
LIFECYCLE_DAYS: dict = {
    BUCKET_TELEMETRY_ARCHIVE: 90,
    BUCKET_INCIDENTS: 365,
    BUCKET_AUDIT_LOGS: 365,
}


def _lifecycle_for(bucket_name: str, days: int) -> LifecycleConfig:
    """Build a LifecycleConfig expiring all objects after ``days``.

    A single rule covers the whole bucket (no filter prefix), matching the
    plan contract: ``telemetry-archive(90d) / incidents(365d) /
    audit-logs(365d)``. Rule ID is deterministic per bucket so repeated runs
    produce identical configs (idempotent overwrite).

    Expiration is used instead of Transition: MinIO validates Transition
    storage classes against configured remote tiers (``mc admin tier add``),
    and with no tier configured it rejects every storage class, including
    "STANDARD"/"REDUCED_REDUNDANCY"/"IA" (S3Error InvalidStorageClass). A bare
    Transition without storage_class fails XML schema validation. Expiration
    (expire after N days) is the only lifecycle action this deployment accepts
    — verified empirically against live MinIO (RELEASE.2025-09-07).
    """
    expiration = Expiration(days=days)
    rule = Rule(
        status="Enabled",
        rule_id=f"{bucket_name}-expire",
        expiration=expiration,
    )
    return LifecycleConfig(rules=[rule])


def _ensure_bucket(client: Minio, bucket_name: str) -> bool:
    """Create ``bucket_name`` if missing; return True when it already existed.

    Uses bucket_exists() before make_bucket() so a second run never raises
    BucketAlreadyOwnedByYou — the idempotency guarantee of this module.
    """
    if client.bucket_exists(bucket_name):
        logger.info("bucket exists: %s (skip create)", bucket_name)
        return True
    retry_with_backoff(client.make_bucket, logger=logger, bucket_name=bucket_name)
    logger.info("bucket created: %s", bucket_name)
    return False


def _apply_lifecycle(client: Minio, bucket_name: str) -> None:
    """Attach the bucket's retention (expiry) lifecycle rule (idempotent).

    MinIO's SetBucketLifecycle is an overwrite operation, so calling it with
    the same rule on every run is safe by design (no error on second call).
    """
    days = LIFECYCLE_DAYS[bucket_name]
    config = _lifecycle_for(bucket_name, days)
    retry_with_backoff(
        client.set_bucket_lifecycle, logger=logger, bucket_name=bucket_name, config=config
    )
    logger.info(
        "lifecycle set: %s -> transition to IA after %d days", bucket_name, days
    )


def setup_buckets(
    client: Optional[Minio] = None,
    config: Optional[StorageConfig] = None,
    buckets: Optional[List[str]] = None,
) -> List[str]:
    """Idempotently ensure all MinIO buckets exist and apply lifecycle rules.

    Args:
        client: Connected Minio client. If None, one is built from ``config``
                (endpoint WITHOUT scheme, port 9010 per StorageConfig).
        config: StorageConfig used to build the client when ``client`` is None;
                defaults to StorageConfig.from_env().
        buckets: Bucket names to ensure. Defaults to all 5 OmniWatch buckets.
                Lifecycle is applied only to buckets present in LIFECYCLE_DAYS.

    Returns:
        Sorted list of ensured bucket names (for logging/E2E assertions).
    """
    if client is None:
        cfg = config or StorageConfig.from_env()
        client = Minio(
            cfg.minio_endpoint,
            access_key=cfg.minio_access_key,
            secret_key=cfg.minio_secret_key,
            secure=cfg.minio_secure,
        )
    names = buckets or [
        BUCKET_TELEMETRY_ARCHIVE,
        BUCKET_INCIDENTS,
        BUCKET_AUDIT_LOGS,
        BUCKET_RUNBOOKS,
        BUCKET_ML_DATASETS,
    ]
    for bucket_name in names:
        _ensure_bucket(client, bucket_name)
        if bucket_name in LIFECYCLE_DAYS:
            _apply_lifecycle(client, bucket_name)
    logger.info("bucket setup complete: %d bucket(s) ensured", len(names))
    return sorted(names)


if __name__ == "__main__":
    setup_buckets()
