"""
Bronze Ingestion Service

Takes confirmed schema + compatible files, reads them via Spark,
and writes to MinIO Bronze layer as partitioned CSV.
"""

import os
import time
import logging
from datetime import datetime, timezone, date
from uuid import UUID
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, FloatType, DoubleType, BooleanType,
    TimestampType, DateType,
)

from backend.app.config import get_settings

logger = logging.getLogger(__name__)

# Our type names → Spark types
SPARK_TYPE_MAP = {
    "string": StringType(),
    "integer": IntegerType(),
    "long": LongType(),
    "float": FloatType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "timestamp": TimestampType(),
    "date": DateType(),
}


def _get_spark_session() -> SparkSession:
    """Create or get a Spark session configured for MinIO."""
    settings = get_settings()

    spark = (
        SparkSession.builder
        .appName("AutonomousPipeline-BronzeIngestion")
        .master(settings.spark_master_url)
        .config("spark.hadoop.fs.s3a.endpoint", settings.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.aws_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", settings.aws_secret_access_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def _build_spark_schema(fields: list[dict]) -> Optional[StructType]:
    """
    Build a Spark StructType from our schema field definitions.
    Returns None if schema should be auto-inferred.
    """
    struct_fields = []
    for field in fields:
        field_name = field.get("new_name") or field["name"]
        field_type = field.get("new_type") or field["detected_type"]
        nullable = field.get("nullable", True)

        spark_type = SPARK_TYPE_MAP.get(field_type, StringType())
        struct_fields.append(StructField(field_name, spark_type, nullable))

    return StructType(struct_fields)


def ingest_to_bronze(
    pipeline_id: UUID,
    schema_version: int,
    file_format: str,
    compatible_files: list[str],
    fields: list[dict],
    csv_delimiter: str = ",",
    csv_header: bool = True,
    csv_encoding: str = "utf-8",
) -> dict:
    """
    Read compatible files and write to MinIO Bronze as CSV.

    Args:
        pipeline_id: Pipeline UUID
        schema_version: Schema version number
        file_format: csv, json, or parquet
        compatible_files: List of absolute file paths to ingest
        fields: Schema field definitions
        csv_delimiter: CSV delimiter character
        csv_header: Whether CSV has header row
        csv_encoding: CSV file encoding

    Returns:
        dict with ingestion results:
        {
            "bronze_path": "s3a://bronze/{pipeline_id}/v{version}/data",
            "total_records": int,
            "files_ingested": [{path, rows, status}],
            "files_skipped": [{path, reason}],
            "duration_seconds": float,
            "status": "success" | "failed",
            "error_message": str | None
        }
    """
    settings = get_settings()
    start_time = time.time()

    bronze_path = f"s3a://{settings.bronze_bucket}/{pipeline_id}/v{schema_version}/data"
    today = date.today().isoformat()

    result = {
        "bronze_path": bronze_path,
        "total_records": 0,
        "files_ingested": [],
        "files_skipped": [],
        "duration_seconds": 0,
        "status": "running",
        "error_message": None,
    }

    spark = None

    try:
        spark = _get_spark_session()

        # Read all compatible files
        all_dfs = []
        for fpath in compatible_files:
            try:
                if file_format == "csv":
                    df = (
                        spark.read
                        .option("header", str(csv_header).lower())
                        .option("sep", csv_delimiter)
                        .option("encoding", csv_encoding)
                        .option("inferSchema", "true")
                        .csv(fpath)
                    )
                elif file_format == "json":
                    df = spark.read.json(fpath)
                elif file_format == "parquet":
                    df = spark.read.parquet(fpath)
                else:
                    result["files_skipped"].append({
                        "path": fpath,
                        "reason": f"Unsupported format: {file_format}",
                    })
                    continue

                row_count = df.count()
                all_dfs.append(df)

                result["files_ingested"].append({
                    "path": fpath,
                    "rows": row_count,
                    "status": "success",
                })
                result["total_records"] += row_count

                logger.info(f"Read {row_count} rows from {fpath}")

            except Exception as e:
                logger.error(f"Failed to read {fpath}: {e}")
                result["files_skipped"].append({
                    "path": fpath,
                    "reason": str(e),
                })

        if not all_dfs:
            result["status"] = "failed"
            result["error_message"] = "No files could be read successfully"
            result["duration_seconds"] = round(time.time() - start_time, 2)
            return result

        # Union all DataFrames
        combined_df = all_dfs[0]
        for df in all_dfs[1:]:
            combined_df = combined_df.unionByName(df, allowMissingColumns=True)

        # Add metadata columns
        combined_df = (
            combined_df
            .withColumn("_ingestion_date", F.lit(today))
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn("_pipeline_id", F.lit(str(pipeline_id)))
            .withColumn("_schema_version", F.lit(schema_version))
        )

        # Write to Bronze (immutable, partitioned by ingestion date) as CSV
        (
            combined_df
            .write
            .mode("append")
            .option("header", "true")
            .partitionBy("_ingestion_date")
            .csv(bronze_path)
        )

        result["status"] = "success"
        logger.info(
            f"Bronze ingestion complete: {result['total_records']} records "
            f"written to {bronze_path}"
        )

    except Exception as e:
        logger.error(f"Bronze ingestion failed: {e}", exc_info=True)
        result["status"] = "failed"
        result["error_message"] = str(e)

    finally:
        if spark:
            spark.stop()

        result["duration_seconds"] = round(time.time() - start_time, 2)

    return result
