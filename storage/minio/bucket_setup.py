"""
OmniWatch — Unified Storage Layer
Component: MinIO Bucket Setup
Phase: 3
Purpose: Creates all required MinIO buckets for OmniWatch storage
Inputs: MinIO connection details from .env
Outputs: 5 created buckets ready for data storage
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")


def setup_buckets():
    """Create all required MinIO buckets."""
    from client import MinIOClient

    client = MinIOClient()
    if client.is_connected():
        client.setup_buckets()
    else:
        print("[bucket_setup] Cannot connect to MinIO")


if __name__ == "__main__":
    setup_buckets()
