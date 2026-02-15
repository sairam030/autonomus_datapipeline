"""
Spark Utilities — reusable helpers for PySpark session management,
MinIO data reading, JSON serialization, and schema introspection.

Used across Silver and Gold services to avoid duplicated Spark boilerplate.
"""

import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# MinIO / S3 credential helpers
# ============================================================================

def get_minio_config() -> dict[str, str]:
    """Return MinIO connection config from environment variables."""
    return {
        "endpoint": os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        "access_key": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        "secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    }


def get_postgres_config() -> dict[str, str]:
    """Return PostgreSQL connection config from environment variables."""
    return {
        "host": os.environ.get("POSTGRES_HOST", "postgres"),
        "port": os.environ.get("POSTGRES_PORT", "5432"),
        "user": os.environ.get("POSTGRES_USER", "pipeline"),
        "password": os.environ.get("POSTGRES_PASSWORD", "pipeline123"),
        "dbname": os.environ.get("POSTGRES_DB", "autonomous_pipeline"),
    }


# ============================================================================
# Spark session builders
# ============================================================================

def _stop_existing_spark():
    """Stop any lingering SparkSession to avoid config bleed."""
    from pyspark.sql import SparkSession
    try:
        existing = SparkSession.getActiveSession()
        if existing:
            existing.stop()
    except Exception:
        pass


def create_local_spark(app_name: str, memory: str = "512m") -> "SparkSession":
    """
    Create a lightweight local Spark session for dry-runs / previews.
    No S3/MinIO config — purely in-memory.
    """
    from pyspark.sql import SparkSession
    _stop_existing_spark()
    return (
        SparkSession.builder
        .master("local[1]")
        .appName(app_name)
        .config("spark.driver.memory", memory)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def create_minio_spark(app_name: str, memory: str = "1g") -> "SparkSession":
    """
    Create a Spark session wired to MinIO via S3A.
    Used for reading/writing Bronze, Silver, Gold data.
    """
    from pyspark.sql import SparkSession
    minio = get_minio_config()
    _stop_existing_spark()
    return (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.driver.memory", memory)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint", minio["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", minio["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", minio["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def create_preview_spark(app_name: str) -> "SparkSession":
    """
    Create a lightweight Spark session wired to MinIO for data previews.
    Uses less memory than the full upload session.
    """
    from pyspark.sql import SparkSession
    minio = get_minio_config()
    _stop_existing_spark()
    return (
        SparkSession.builder
        .master("local[1]")
        .appName(app_name)
        .config("spark.driver.memory", "512m")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint", minio["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", minio["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", minio["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


# ============================================================================
# S3 bucket helpers
# ============================================================================

def ensure_bucket_exists(bucket_name: str) -> None:
    """Create an S3/MinIO bucket if it doesn't already exist."""
    import boto3
    minio = get_minio_config()
    s3 = boto3.client(
        "s3",
        endpoint_url=minio["endpoint"],
        aws_access_key_id=minio["access_key"],
        aws_secret_access_key=minio["secret_key"],
    )
    try:
        s3.head_bucket(Bucket=bucket_name)
    except Exception:
        s3.create_bucket(Bucket=bucket_name)


# ============================================================================
# JSON-safe value conversion
# ============================================================================

def json_safe(val: Any) -> Any:
    """Convert a value to something JSON-serialisable."""
    import datetime as _dt
    from decimal import Decimal as _Decimal

    if val is None:
        return None
    if isinstance(val, (_dt.datetime, _dt.date)):
        return val.isoformat()
    if isinstance(val, _dt.time):
        return val.isoformat()
    if isinstance(val, _Decimal):
        return float(val)
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    if not isinstance(val, (str, int, float, bool)):
        return str(val)
    return val


# ============================================================================
# Pipeline slug generation
# ============================================================================

def make_pipeline_slug(pipeline_name: str) -> str:
    """Convert a pipeline name to a URL/path-safe slug."""
    import re
    return re.sub(r"[^a-z0-9]+", "_", pipeline_name.lower()).strip("_")


# ============================================================================
# Silver schema introspection — used by Gold layer
# ============================================================================

def get_silver_schema_and_samples(db, project_id) -> tuple[list, list]:
    """
    Read the latest Silver execution output and return (schema, sample_rows).

    Raises ValueError if no completed Silver execution exists
    (Gold must always work on Silver output, never raw Bronze).
    """
    from backend.app.models.models import SilverExecution

    latest_exec = (
        db.query(SilverExecution)
        .filter(SilverExecution.pipeline_id == project_id, SilverExecution.status == "completed")
        .order_by(SilverExecution.created_at.desc())
        .first()
    )
    if not latest_exec or not latest_exec.output_path:
        raise ValueError(
            "No completed Silver execution found for this project. "
            "Run 'Upload to Silver' first so Gold receives the correct "
            "(post-transformation) schema."
        )

    spark = None
    try:
        spark = create_preview_spark("schema_detect")
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(latest_exec.output_path)

        schema = [
            {
                "name": f.name,
                "detected_type": str(f.dataType).replace("Type", "").lower(),
                "type": str(f.dataType),
                "nullable": f.nullable,
                "sample_values": [],
            }
            for f in df.schema.fields
        ]

        sample_rows_raw = [
            {k: json_safe(v) for k, v in row.asDict().items()}
            for row in df.limit(10).collect()
        ]

        for field_info in schema:
            fname = field_info["name"]
            field_info["sample_values"] = [
                row.get(fname) for row in sample_rows_raw
            ]

        return schema, sample_rows_raw
    except ValueError:
        raise
    except Exception as e:
        logger.warning("Could not read Silver schema: %s", e)
        raise ValueError(
            "No completed Silver execution found for this project. "
            "Run 'Upload to Silver' first so Gold receives the correct "
            "(post-transformation) schema."
        ) from e
    finally:
        if spark:
            spark.stop()


# ============================================================================
# Data preview helper
# ============================================================================

def preview_data_from_path(data_path: str, limit: int = 50) -> dict:
    """
    Read CSV data from an S3A path and return preview information.

    Returns dict with: total_records, columns, rows, preview_count
    """
    spark = None
    try:
        spark = create_preview_spark("data_preview")
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(data_path)

        total_count = df.count()
        schema_info = [
            {"name": f.name, "type": str(f.dataType), "nullable": f.nullable}
            for f in df.schema.fields
        ]
        sample_rows = [
            {k: json_safe(v) for k, v in row.asDict().items()}
            for row in df.limit(limit).collect()
        ]

        return {
            "total_records": total_count,
            "columns": schema_info,
            "rows": sample_rows,
            "preview_count": len(sample_rows),
        }
    finally:
        if spark:
            spark.stop()
