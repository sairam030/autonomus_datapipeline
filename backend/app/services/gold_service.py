"""
Gold Service — PySpark execution logic for Gold layer uploads
and push-to-Postgres operations.

Reads Silver data from MinIO, applies confirmed Gold transformations
sequentially, writes to the Gold bucket, and optionally pushes to Postgres.
"""

import logging
import re
import time

from sqlalchemy.orm import Session

from backend.app.models.models import SilverExecution
from backend.app.services.sandbox import build_safe_exec_globals
from backend.app.services.spark_utils import (
    create_minio_spark, ensure_bucket_exists, get_postgres_config, make_pipeline_slug,
)
from backend.app.services.code_saver import save_gold_upload_pipeline

logger = logging.getLogger(__name__)


# ============================================================================
# Postgres helpers
# ============================================================================

def sanitize_table_name(name: str) -> str:
    """Validate and sanitize a table name to prevent SQL injection."""
    clean = name.strip().strip('"')
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', clean):
        raise ValueError(
            f"Invalid table name '{name}'. Only letters, digits, underscores, "
            f"and dots are allowed, and it must start with a letter or underscore."
        )
    if len(clean) > 63:
        raise ValueError(f"Table name too long ({len(clean)} chars). Max is 63.")
    return clean


def _table_exists(cursor, table_name: str) -> bool:
    """Check if a table exists in the public schema."""
    cursor.execute(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name=%s)",
        (table_name,),
    )
    return cursor.fetchone()[0]


# ============================================================================
# Gold upload — apply transforms to Silver data
# ============================================================================

def execute_gold_upload(pipeline, transforms: list, db: Session) -> dict:
    """
    Read Silver data from MinIO, apply all Gold transformations sequentially,
    write result to Gold bucket as CSV.

    Args:
        pipeline: Pipeline ORM object
        transforms: list of GoldTransformation objects with confirmed_code
        db: database session

    Returns:
        dict with keys: input_path, output_path, input_records,
                        output_records, transform_results
    """
    pipeline_slug = make_pipeline_slug(pipeline.name)
    gold_bucket = "gold"
    gold_path = f"s3a://{gold_bucket}/{pipeline_slug}/gold/"

    # Find the Silver output path
    latest_silver = (
        db.query(SilverExecution)
        .filter(SilverExecution.pipeline_id == pipeline.id, SilverExecution.status == "completed")
        .order_by(SilverExecution.created_at.desc())
        .first()
    )
    if not latest_silver or not latest_silver.output_path:
        raise ValueError("No completed Silver execution found. Run Upload to Silver first.")

    silver_path = latest_silver.output_path

    spark = None
    try:
        spark = create_minio_spark(f"gold_upload_{pipeline_slug}")
        ensure_bucket_exists(gold_bucket)

        # Read Silver data
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(silver_path))

        input_count = df.count()

        # Save pipeline code reference
        try:
            save_gold_upload_pipeline(
                project_name=pipeline.name,
                transforms=[
                    {"name": t.name, "code": t.confirmed_code, "version": t.version}
                    for t in transforms
                ],
                silver_path=silver_path,
                gold_path=gold_path,
            )
        except Exception as save_err:
            logger.warning("Could not save gold upload pipeline code to disk: %s", save_err)

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
                        f"Gold Transform '{t.name}' (v{t.version}) has no `transform` function"
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
                    f"Gold Transform '{t.name}' (v{t.version}) failed: {str(te)}"
                ) from te

        output_count = df.count()

        # Write to gold bucket as CSV
        df.write.mode("overwrite").option("header", "true").csv(gold_path)

        return {
            "input_path": silver_path,
            "output_path": gold_path,
            "input_records": input_count,
            "output_records": output_count,
            "transform_results": transform_results,
        }

    finally:
        if spark:
            spark.stop()


# ============================================================================
# Push to Postgres
# ============================================================================

def execute_push_to_postgres(
    gold_path: str,
    table_name: str,
    if_exists: str = "replace",
) -> int:
    """
    Read Gold data from MinIO and push it to a PostgreSQL table.

    Uses PySpark for reading, then pandas + psycopg2 COPY for writing
    (pandas 2.2+ is incompatible with SQLAlchemy 1.4 Engine/Connection).

    Args:
        gold_path: S3A path to Gold data
        table_name: target Postgres table name
        if_exists: "replace", "append", or "fail"

    Returns:
        number of records pushed
    """
    table_name = sanitize_table_name(table_name)
    pg = get_postgres_config()

    spark = None
    try:
        spark = create_minio_spark("push_to_postgres")

        # Read Gold data
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(gold_path)

        # Convert to pandas
        pdf = df.toPandas()
        record_count = len(pdf)

        if record_count == 0:
            raise ValueError("Gold data is empty. Nothing to push.")

        # Write to Postgres using psycopg2 raw connection + COPY
        import psycopg2
        from io import StringIO

        pg_conn = psycopg2.connect(
            host=pg["host"], port=pg["port"], user=pg["user"],
            password=pg["password"], dbname=pg["dbname"],
        )
        try:
            cur = pg_conn.cursor()

            if if_exists == "replace":
                cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                pg_conn.commit()

            # Build CREATE TABLE from pandas dtypes
            dtype_map = {
                "int64": "BIGINT",
                "float64": "DOUBLE PRECISION",
                "object": "TEXT",
                "bool": "BOOLEAN",
                "datetime64[ns]": "TIMESTAMP",
            }
            cols_ddl = ", ".join(
                f'"{col}" {dtype_map.get(str(pdf[col].dtype), "TEXT")}'
                for col in pdf.columns
            )

            if if_exists in ("replace",) or not _table_exists(cur, table_name):
                cur.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({cols_ddl})')
                pg_conn.commit()

            # COPY via StringIO for speed
            buf = StringIO()
            pdf.to_csv(buf, index=False, header=False, na_rep="\\N")
            buf.seek(0)
            cur.copy_expert(
                f'COPY "{table_name}" FROM STDIN WITH (FORMAT CSV, NULL \'\\N\')',
                buf,
            )
            pg_conn.commit()
            cur.close()
        finally:
            pg_conn.close()

        logger.info("Pushed %d records to Postgres table '%s'", record_count, table_name)
        return record_count

    finally:
        if spark:
            spark.stop()
