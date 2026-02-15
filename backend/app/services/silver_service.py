"""
Silver Service — PySpark execution logic for Silver layer uploads.

Reads Bronze data from MinIO, applies confirmed Silver transformations
sequentially, and writes the result to the Silver bucket.
"""

import logging
import time

from sqlalchemy.orm import Session

from backend.app.models.models import BronzeIngestion
from backend.app.services.sandbox import build_safe_exec_globals
from backend.app.services.spark_utils import (
    create_minio_spark, ensure_bucket_exists, make_pipeline_slug,
)
from backend.app.services.code_saver import save_silver_upload_pipeline

logger = logging.getLogger(__name__)


def execute_silver_upload(pipeline, transforms: list, db: Session) -> dict:
    """
    Read bronze data from MinIO, apply all Silver transformations sequentially,
    write result to Silver bucket as CSV.

    Args:
        pipeline: Pipeline ORM object
        transforms: list of SilverTransformation objects with confirmed_code
        db: database session (for querying latest bronze ingestion)

    Returns:
        dict with keys: input_path, output_path, input_records,
                        output_records, transform_results
    """
    pipeline_slug = make_pipeline_slug(pipeline.name)
    silver_bucket = "silver"
    silver_path = f"s3a://{silver_bucket}/{pipeline_slug}/silver/"

    spark = None
    try:
        spark = create_minio_spark(f"silver_upload_{pipeline_slug}")
        ensure_bucket_exists(silver_bucket)

        # ---------- locate the latest bronze version path ----------
        latest_ingestion = (
            db.query(BronzeIngestion)
            .filter(BronzeIngestion.pipeline_id == pipeline.id)
            .order_by(BronzeIngestion.created_at.desc())
            .first()
        )

        bronze_path = None
        if latest_ingestion and latest_ingestion.bronze_path:
            bronze_path = latest_ingestion.bronze_path

        if not bronze_path:
            raise ValueError('No Bronze ingestion found. Run Bronze ingestion first.')

        # Guard: Kafka/API Bronze ingestions that haven't run yet
        if latest_ingestion and latest_ingestion.status == "pending":
            raise ValueError(
                'Bronze data has not arrived yet. The Kafka/API Bronze DAG '
                'must run at least once before you can upload to Silver. '
                'Trigger the Bronze DAG in Airflow first.'
            )

        # ---------- read bronze data (auto-detect format) ----------
        try:
            df = spark.read.parquet(bronze_path)
            logger.info("Read bronze data as Parquet from %s", bronze_path)
        except Exception:
            df = (spark.read
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .csv(bronze_path))
            logger.info("Read bronze data as CSV from %s", bronze_path)

        input_count = df.count()

        # Save pipeline code reference
        try:
            save_silver_upload_pipeline(
                project_name=pipeline.name,
                transforms=[
                    {"name": t.name, "code": t.confirmed_code, "version": t.version}
                    for t in transforms
                ],
                bronze_path=bronze_path,
                silver_path=silver_path,
            )
        except Exception as save_err:
            logger.warning("Could not save upload pipeline code to disk: %s", save_err)

        # Apply each transformation in order
        transform_results = []
        for t in transforms:
            t_start = time.time()
            try:
                exec_globals = build_safe_exec_globals()
                exec(t.confirmed_code, exec_globals)
                transform_fn = exec_globals.get("transform")
                if not transform_fn:
                    raise ValueError(
                        f"Transform '{t.name}' (v{t.version}) has no `transform` function"
                    )
                df = transform_fn(df, spark)
                transform_results.append({
                    "id": str(t.id),
                    "name": t.name,
                    "version": t.version,
                    "status": "success",
                    "duration_seconds": round(time.time() - t_start, 2),
                })
            except Exception as te:
                transform_results.append({
                    "id": str(t.id),
                    "name": t.name,
                    "version": t.version,
                    "status": "failed",
                    "error": str(te),
                    "duration_seconds": round(time.time() - t_start, 2),
                })
                raise ValueError(
                    f"Transform '{t.name}' (v{t.version}) failed: {str(te)}"
                ) from te

        output_count = df.count()

        # Write to silver bucket as CSV
        df.write.mode("overwrite").option("header", "true").csv(silver_path)

        return {
            "input_path": bronze_path,
            "output_path": silver_path,
            "input_records": input_count,
            "output_records": output_count,
            "transform_results": transform_results,
        }

    finally:
        if spark:
            spark.stop()
