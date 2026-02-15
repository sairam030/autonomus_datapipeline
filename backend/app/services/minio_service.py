"""
MinIO/S3 utility functions for the pipeline.
"""

import io
import logging
from minio import Minio
from backend.app.config import get_settings

logger = logging.getLogger(__name__)


def get_minio_client() -> Minio:
    """Create a MinIO client from settings."""
    settings = get_settings()
    endpoint = settings.minio_endpoint.replace("http://", "").replace("https://", "")
    secure = settings.minio_endpoint.startswith("https://")

    return Minio(
        endpoint,
        access_key=settings.aws_access_key_id,
        secret_key=settings.aws_secret_access_key,
        secure=secure,
    )


def ensure_bucket(bucket_name: str):
    """Create bucket if it doesn't exist."""
    client = get_minio_client()
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        logger.info(f"Created bucket: {bucket_name}")
