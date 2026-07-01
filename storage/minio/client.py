"""
OmniWatch — Unified Storage Layer
Component: MinIO Client
Phase: 3
Purpose: MinIO read/write client for object storage (archive, incidents, audit logs)
Inputs: JSON objects, files, reports
Outputs: MinIO buckets with stored objects
"""

import json
import os
import sys
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------------
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9010")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# All required buckets
REQUIRED_BUCKETS = [
    "omniwatch-telemetry-archive",
    "omniwatch-incidents",
    "omniwatch-audit-logs",
    "omniwatch-ml-datasets",
    "omniwatch-runbooks",
]


class MinIOClient:
    """
    MinIO client for OmniWatch object storage.

    Usage:
        client = MinIOClient()
        client.upload_json("omniwatch-incidents", "inc-123.json", data)
        data = client.download_json("omniwatch-incidents", "inc-123.json")
    """

    def __init__(self):
        """Initialize MinIO connection."""
        self._client = None
        self._connected = False
        self._connect()

    def _connect(self):
        """Establish connection to MinIO."""
        try:
            from minio import Minio
            self._client = Minio(
                MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=False,
            )
            # Test connection
            self._client.list_buckets()
            self._connected = True
            print(f"[minio] Connected to {MINIO_ENDPOINT}")
        except ImportError:
            print("[minio] WARNING: minio not installed. Install with: pip install minio")
            self._connected = False
        except Exception as e:
            print(f"[minio] WARNING: Connection failed: {e}")
            self._connected = False

    def is_connected(self) -> bool:
        """Check if connected to MinIO."""
        return self._connected

    def setup_buckets(self):
        """Create all required buckets if they don't exist."""
        if not self._connected:
            print("[minio] Not connected — cannot setup buckets")
            return False

        try:
            existing = [b.name for b in self._client.list_buckets()]
            for bucket in REQUIRED_BUCKETS:
                if bucket not in existing:
                    self._client.make_bucket(bucket)
                    print(f"[minio] Created bucket: {bucket}")
                else:
                    print(f"[minio] Bucket exists: {bucket}")
            return True
        except Exception as e:
            print(f"[minio] Bucket setup failed: {e}")
            return False

    def upload_json(self, bucket: str, key: str, data: dict) -> bool:
        """
        Upload a JSON object to MinIO.

        Args:
            bucket: Bucket name
            key: Object key (filename)
            data: Dictionary to store as JSON

        Returns:
            True if successful
        """
        if not self._connected:
            return False

        try:
            json_bytes = json.dumps(data, default=str).encode("utf-8")
            stream = BytesIO(json_bytes)
            self._client.put_object(
                bucket,
                key,
                stream,
                length=len(json_bytes),
                content_type="application/json",
            )
            return True
        except Exception as e:
            print(f"[minio] Upload failed: {e}")
            return False

    def download_json(self, bucket: str, key: str) -> Optional[dict]:
        """
        Download a JSON object from MinIO.

        Args:
            bucket: Bucket name
            key: Object key

        Returns:
            Parsed JSON dict or None
        """
        if not self._connected:
            return None

        try:
            response = self._client.get_object(bucket, key)
            data = json.loads(response.read().decode("utf-8"))
            response.close()
            response.release_conn()
            return data
        except Exception as e:
            print(f"[minio] Download failed: {e}")
            return None

    def upload_file(self, bucket: str, key: str, file_path: str, content_type: str = "application/octet-stream") -> bool:
        """Upload a file to MinIO."""
        if not self._connected:
            return False

        try:
            self._client.fput_object(bucket, key, file_path, content_type=content_type)
            return True
        except Exception as e:
            print(f"[minio] File upload failed: {e}")
            return False

    def download_file(self, bucket: str, key: str, file_path: str) -> bool:
        """Download a file from MinIO."""
        if not self._connected:
            return False

        try:
            self._client.fget_object(bucket, key, file_path)
            return True
        except Exception as e:
            print(f"[minio] File download failed: {e}")
            return False

    def list_objects(self, bucket: str, prefix: str = "") -> list:
        """List objects in a bucket with optional prefix."""
        if not self._connected:
            return []

        try:
            objects = self._client.list_objects(bucket, prefix=prefix)
            return [
                {
                    "name": obj.object_name,
                    "size": obj.size,
                    "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
                }
                for obj in objects
            ]
        except Exception as e:
            print(f"[minio] List failed: {e}")
            return []

    def delete_object(self, bucket: str, key: str) -> bool:
        """Delete an object from MinIO."""
        if not self._connected:
            return False

        try:
            self._client.remove_object(bucket, key)
            return True
        except Exception as e:
            print(f"[minio] Delete failed: {e}")
            return False

    def get_bucket_info(self) -> list:
        """Get info about all required buckets."""
        if not self._connected:
            return []

        try:
            buckets = self._client.list_buckets()
            return [
                {
                    "name": b.name,
                    "created": b.creation_date.isoformat() if b.creation_date else None,
                }
                for b in buckets
                if b.name in REQUIRED_BUCKETS
            ]
        except Exception:
            return []


# ---------------------------------------------------------------------------
# CLI interface
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="OmniWatch MinIO Client")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("setup", help="Create all required buckets")
    subparsers.add_parser("status", help="Check connection and bucket info")

    list_parser = subparsers.add_parser("list", help="List objects in bucket")
    list_parser.add_argument("--bucket", required=True, help="Bucket name")

    args = parser.parse_args()
    client = MinIOClient()

    if args.command == "setup":
        client.setup_buckets()

    elif args.command == "status":
        print(f"Connected: {client.is_connected()}")
        if client.is_connected():
            info = client.get_bucket_info()
            print(f"Buckets ({len(info)}):")
            for b in info:
                print(f"  {b['name']}")

    elif args.command == "list":
        objects = client.list_objects(args.bucket)
        print(f"Objects in {args.bucket} ({len(objects)}):")
        for obj in objects:
            print(f"  {obj['name']} ({obj['size']} bytes)")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
