"""
OmniWatch — Generative AI Layer
Component: MinIO Object Store
Phase: 10
Purpose: Auto-creates 3 buckets (runbooks, reports, summaries) and persists
         generated artifacts with multi-tenant keys.
Inputs: GroundedArtifact / Runbook / PostMortem / GeneratedReport
Outputs: MinIO object paths (bucket/key)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from minio import Minio

from genai.models import GroundedArtifact
from genai.settings import Settings

logger = logging.getLogger(__name__)

_BUCKETS: dict[str, str] = {
    "summary": "omniwatch-runbooks",
    "runbook": "omniwatch-runbooks",
    "report": "omniwatch-runbooks",
    "postmortem": "omniwatch-runbooks",
}


class MinioStore:
    """Wraps a MinIO client with auto-creation of required buckets
    and multi-tenant key generation for generated artifacts."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or Settings()
        self._client = Minio(
            self._settings.minio_endpoint,
            access_key=self._settings.minio_access_key,
            secret_key=self._settings.minio_secret_key,
            secure=self._settings.minio_secure,
        )
        if self._settings.minio_auto_create:
            self._ensure_buckets()

    def _ensure_buckets(self) -> None:
        """Create buckets if they do not already exist."""
        bucket_names = {v for v in _BUCKETS.values()}
        for bucket in bucket_names:
            if not self._client.bucket_exists(bucket):
                self._client.make_bucket(bucket)
                logger.info(json.dumps({"event": "bucket_created", "bucket": bucket}))

    def _key_for(self, artifact: GroundedArtifact) -> str:
        """Build a multi-tenant object key from the artifact metadata."""
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
        tenant = artifact.incident_id.replace("/", "_").replace("\\", "_")
        return f"genai/{tenant}/{artifact.artifact_type}/{ts}.json"

    def persist(self, artifact: GroundedArtifact) -> str:
        """Persist an artifact to MinIO and return the object path.

        Args:
            artifact: The generated artifact to store.

        Returns:
            The full ``bucket/key`` path of the stored object.
        """
        bucket = _BUCKETS.get(artifact.artifact_type, "omniwatch-runbooks")
        key = self._key_for(artifact)
        data = artifact.model_dump_json(indent=2).encode("utf-8")

        from io import BytesIO

        self._client.put_object(
            bucket,
            key,
            BytesIO(data),
            length=len(data),
            content_type="application/json",
        )
        path = f"{bucket}/{key}"
        logger.info(json.dumps({"event": "artifact_persisted", "path": path}))
        return path

    def retrieve(self, bucket: str, key: str) -> dict[str, Any]:
        """Retrieve a JSON artifact from MinIO."""
        resp = self._client.get_object(bucket, key)
        try:
            return json.loads(resp.read().decode("utf-8"))
        finally:
            resp.close()
            resp.release_conn()

    def list_artifacts(self, bucket: str, prefix: str = "genai/") -> list[str]:
        """List object keys under a given prefix."""
        objects = self._client.list_objects(bucket, prefix=prefix)
        return [obj.object_name for obj in objects if obj.object_name is not None]
