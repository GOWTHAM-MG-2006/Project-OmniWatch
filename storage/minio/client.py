"""
OmniWatch — Storage Layer
Component: MinIO Client
Phase: 5
Purpose: Unified object-storage client for MinIO (S3-compatible) providing
         upload / download / list / delete of archived objects and a health
         probe, over the MinIO API port 9010 (NOT the console port 9001).
Inputs: StorageConfig (minio_endpoint host:port, access/secret keys, secure
        flag), raw byte payloads to store, and object keys to fetch/remove
Outputs: ObjectWriteResult on upload, raw object bytes on download, object
         name listings, None on delete, and a health_check() boolean against
         MinIO. All SDK exceptions are converted to StorageError after
         3x exponential backoff retries (100ms -> 500ms -> 2s).
"""

from __future__ import annotations

import io
import logging
from typing import List, NoReturn, Optional

from minio import Minio
from minio.helpers import ObjectWriteResult

from storage.common import StorageError, create_logger, retry_with_backoff
from storage.config import StorageConfig

# AGENTS.md MinIO buckets: this client consumes them; bucket_setup.py (a
# parallel task) owns their creation. Used as the primary health probe bucket.
HEALTH_CHECK_BUCKET: str = "omniwatch-telemetry-archive"

# Matches the 3x exponential backoff contract in storage/common.py.
_RETRIES: int = 3
_BASE_DELAY: float = 0.1
_MAX_DELAY: float = 2.0


class MinioClient:
    """Thin, resilient wrapper around the MinIO Python SDK.

    A single MinioClient instance is safe to share process-wide — the SDK
    manages its own HTTP connection pool internally (the plan's "connection
    pooling" requirement). Every operation runs inside ``retry_with_backoff``
    and any surviving SDK exception is re-raised as ``StorageError``.

    The SDK endpoint is ``host:port`` WITHOUT a scheme (e.g. ``localhost:9010``);
    the ``secure`` flag selects http vs https. Buckets are NOT created here —
    that is bucket_setup.py's responsibility.
    """

    def __init__(
        self,
        config: Optional[StorageConfig] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._config = config or StorageConfig.from_env()
        self._logger = logger or create_logger("omniwatch.storage.minio")
        self._client = Minio(
            endpoint=self._config.minio_endpoint,
            access_key=self._config.minio_access_key,
            secret_key=self._config.minio_secret_key,
            secure=self._config.minio_secure,
        )

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def upload_object(
        self,
        bucket: str,
        object_name: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> ObjectWriteResult:
        """Upload ``data`` bytes to ``bucket/object_name``.

        Returns the SDK ``ObjectWriteResult`` (etag / version_id / location)
        so callers can track what was written. The payload is re-seeked before
        each retry attempt — without this a retried upload would send 0 bytes
        because ``io.BytesIO`` advances past EOF.
        """
        payload = io.BytesIO(data)

        def _attempt() -> ObjectWriteResult:
            payload.seek(0)
            return self._client.put_object(
                bucket,
                object_name,
                payload,
                length=len(data),
                content_type=content_type,
            )

        try:
            return retry_with_backoff(
                _attempt,
                retries=_RETRIES,
                base_delay=_BASE_DELAY,
                max_delay=_MAX_DELAY,
                logger=self._logger,
            )
        except Exception as exc:
            self._raise_storage_error("upload", f"{bucket}/{object_name}", exc)

    def download_object(self, bucket: str, object_name: str) -> bytes:
        """Fetch ``bucket/object_name`` and return its raw bytes.

        The SDK response stream is always closed (and its connection released)
        after reading, even on partial-read failure; a retried attempt fetches
        a fresh response. Task 16 byte-compares this against what was uploaded,
        so the returned value is the raw bytes exactly as stored.
        """
        def _attempt() -> bytes:
            response = self._client.get_object(bucket, object_name)
            try:
                return response.read()
            finally:
                response.close()
                response.release_conn()

        try:
            return retry_with_backoff(
                _attempt,
                retries=_RETRIES,
                base_delay=_BASE_DELAY,
                max_delay=_MAX_DELAY,
                logger=self._logger,
            )
        except Exception as exc:
            self._raise_storage_error("download", f"{bucket}/{object_name}", exc)

    def list_objects(self, bucket: str, prefix: str = "") -> List[str]:
        """Return the object names in ``bucket`` under ``prefix`` (recursive).

        Uses ``list_objects(recursive=True)`` so results are not confined to a
        single "directory" delimiter level — the plan's archive buckets store
        objects at arbitrary nested paths.
        """
        def _attempt() -> List[str]:
            return [
                obj.object_name
                for obj in self._client.list_objects(
                    bucket, prefix=prefix, recursive=True
                )
                if obj.object_name is not None
            ]

        try:
            return retry_with_backoff(
                _attempt,
                retries=_RETRIES,
                base_delay=_BASE_DELAY,
                max_delay=_MAX_DELAY,
                logger=self._logger,
            )
        except Exception as exc:
            self._raise_storage_error("list", f"{bucket}/{prefix!r}", exc)

    def delete_object(self, bucket: str, object_name: str) -> None:
        """Remove ``bucket/object_name``. Returns None on success."""
        def _attempt() -> None:
            self._client.remove_object(bucket, object_name)

        try:
            retry_with_backoff(
                _attempt,
                retries=_RETRIES,
                base_delay=_BASE_DELAY,
                max_delay=_MAX_DELAY,
                logger=self._logger,
            )
        except Exception as exc:
            self._raise_storage_error("delete", f"{bucket}/{object_name}", exc)

    def health_check(self) -> bool:
        """Return True if MinIO is reachable and credentials are valid.

        Primary probe is ``bucket_exists`` on ``omniwatch-telemetry-archive``
        (AGENTS.md bucket contract). Because bucket_setup.py runs in parallel
        and may not have created buckets yet, a ``bucket_exists`` miss falls
        back to a ``list_buckets`` connectivity probe — a successful listing
        proves endpoint + credentials work regardless of bucket state. Never
        raises; returns False after retries are exhausted.
        """
        try:
            retry_with_backoff(
                self._probe,
                retries=_RETRIES,
                base_delay=_BASE_DELAY,
                max_delay=_MAX_DELAY,
                logger=self._logger,
            )
            return True
        except Exception as exc:
            self._logger.error("health_check failed: %s", exc)
            return False

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #
    def _probe(self) -> None:
        """Single health probe; raises on connectivity/credential failure."""
        if self._client.bucket_exists(HEALTH_CHECK_BUCKET):
            return
        # Bucket absent (bucket_setup not run yet) — listing proves the
        # endpoint + credentials are still healthy.
        self._client.list_buckets()

    def _raise_storage_error(self, operation: str, target: str, exc: Exception) -> NoReturn:
        """Translate any surviving SDK exception into StorageError."""
        raise StorageError(f"MinIO {operation} failed for {target}: {exc}") from exc
