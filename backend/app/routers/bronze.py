"""
Bronze ingestion status/tracking API routes.
"""

import logging
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from backend.app.database import get_db
from backend.app.models.models import BronzeIngestion
from backend.app.schemas.bronze import BronzeIngestionResponse, BronzeIngestionStatus

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/bronze", tags=["Bronze Ingestion"])


@router.get("/{pipeline_id}/ingestions", response_model=list[BronzeIngestionResponse])
def list_ingestions(pipeline_id: UUID, db: Session = Depends(get_db)):
    """List all Bronze ingestion runs for a pipeline."""
    ingestions = (
        db.query(BronzeIngestion)
        .filter(BronzeIngestion.pipeline_id == pipeline_id)
        .order_by(BronzeIngestion.created_at.desc())
        .all()
    )
    return ingestions


@router.get("/ingestion/{ingestion_id}", response_model=BronzeIngestionResponse)
def get_ingestion(ingestion_id: UUID, db: Session = Depends(get_db)):
    """Get details of a specific Bronze ingestion run."""
    ingestion = db.query(BronzeIngestion).filter(BronzeIngestion.id == ingestion_id).first()
    if not ingestion:
        raise HTTPException(404, "Ingestion not found")
    return ingestion


@router.get("/ingestion/{ingestion_id}/status", response_model=BronzeIngestionStatus)
def get_ingestion_status(ingestion_id: UUID, db: Session = Depends(get_db)):
    """Lightweight status check for a Bronze ingestion (for polling from UI)."""
    ingestion = db.query(BronzeIngestion).filter(BronzeIngestion.id == ingestion_id).first()
    if not ingestion:
        raise HTTPException(404, "Ingestion not found")

    return BronzeIngestionStatus(
        ingestion_id=ingestion.id,
        pipeline_id=ingestion.pipeline_id,
        status=ingestion.status,
        total_records=ingestion.total_records or 0,
        files_processed=len(ingestion.files_ingested or []),
        files_skipped=len(ingestion.files_skipped or []),
        duration_seconds=ingestion.duration_seconds or 0,
        error_message=ingestion.error_message,
    )


_spark_preview = None


def _get_preview_spark():
    """Get or create a reusable SparkSession for preview operations."""
    import os
    from pyspark.sql import SparkSession

    global _spark_preview
    # Reuse existing session if still alive
    if _spark_preview is not None:
        try:
            _spark_preview.sparkContext._jsc.sc().isStopped()
            return _spark_preview
        except Exception:
            _spark_preview = None

    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_access = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    minio_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

    _spark_preview = (
        SparkSession.builder
        .master("local[1]")
        .appName("bronze_preview")
        .config("spark.driver.memory", "512m")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    _spark_preview.sparkContext.setLogLevel("WARN")
    return _spark_preview


@router.post("/{pipeline_id}/ingestion-complete")
def mark_ingestion_complete(
    pipeline_id: UUID,
    db: Session = Depends(get_db),
    total_records: int = 0,
    bronze_path: str = "",
):
    """
    Called by the Airflow Kafka/API Bronze DAG after successfully writing data.
    Updates the BronzeIngestion record from 'pending' to 'success'.
    """
    from datetime import datetime, timezone

    ingestion = (
        db.query(BronzeIngestion)
        .filter(BronzeIngestion.pipeline_id == pipeline_id)
        .order_by(BronzeIngestion.created_at.desc())
        .first()
    )
    if not ingestion:
        raise HTTPException(404, "No Bronze ingestion record found")

    ingestion.status = "success"
    ingestion.total_records = (ingestion.total_records or 0) + total_records
    ingestion.files_ingested = (ingestion.files_ingested or 0) + 1
    ingestion.completed_at = datetime.now(timezone.utc)
    if bronze_path:
        ingestion.bronze_path = bronze_path
    db.commit()

    logger.info(
        "Bronze ingestion marked complete for pipeline %s: %d records",
        pipeline_id, total_records,
    )
    return {"status": "success", "total_records": ingestion.total_records}


@router.get("/{pipeline_id}/preview")
def preview_bronze_data(
    pipeline_id: UUID,
    limit: int = 50,
    db: Session = Depends(get_db),
):
    """Preview sample rows from the latest Bronze ingestion data."""
    ingestion = (
        db.query(BronzeIngestion)
        .filter(BronzeIngestion.pipeline_id == pipeline_id, BronzeIngestion.status == "success")
        .order_by(BronzeIngestion.created_at.desc())
        .first()
    )
    if not ingestion:
        raise HTTPException(404, "No successful Bronze ingestion found")

    bronze_path = ingestion.bronze_path
    if not bronze_path:
        raise HTTPException(404, "Bronze path not available")

    try:
        spark = _get_preview_spark()
        # Auto-detect format: try Parquet first, fall back to CSV
        try:
            df = spark.read.parquet(bronze_path)
        except Exception:
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(bronze_path)
        schema_info = [{"name": f.name, "type": str(f.dataType), "nullable": f.nullable} for f in df.schema.fields]
        sample_rows = [row.asDict() for row in df.limit(limit).collect()]

        # Use stored total_records from ingestion if available to avoid expensive count()
        total_count = ingestion.total_records if ingestion.total_records else len(sample_rows)

        # Stringify values for JSON serialization
        for row in sample_rows:
            for k, v in row.items():
                if v is not None and not isinstance(v, (str, int, float, bool)):
                    row[k] = str(v)

        return {
            "total_records": total_count,
            "columns": schema_info,
            "rows": sample_rows,
            "bronze_path": bronze_path,
            "preview_count": len(sample_rows),
        }
    except Exception as e:
        raise HTTPException(500, f"Failed to preview Bronze data: {str(e)}")
