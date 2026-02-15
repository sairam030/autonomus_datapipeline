"""
Auto-generated Bronze Ingestion DAG — File Source
Project : flight
Source   : csv files
Generated: 2026-02-14 13:42:33 UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os, json, logging

logger = logging.getLogger(__name__)

default_args = {
    "owner": "autonomous-pipeline",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def create_buckets(**ctx):
    """Ensure MinIO buckets exist."""
    from minio import Minio
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "").replace("https://", "")
    client = Minio(endpoint, access_key=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
                   secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"), secure=False)
    for bucket in ["bronze", "silver", "gold"]:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            logger.info("Created bucket: %s", bucket)


def ingest_bronze(**ctx):
    """Read source files with Spark and write to MinIO Bronze layer."""
    from pyspark.sql import SparkSession

    config = {'csv_header': True, 'csv_encoding': 'utf-8', 'csv_delimiter': ',', 'file_path': '/data/pipeline/986ed019-03ec-4df8-b8df-2bc274f95a9c', 'file_format': 'csv'}
    pipeline_id = "986ed019-03ec-4df8-b8df-2bc274f95a9c"
    source_type = "csv"

    minio_ep = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    ak = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    sk = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
    bronze_path = f"s3a://bronze/{pipeline_id}/{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    spark = (
        SparkSession.builder
        .appName(f"bronze_ingest_{pipeline_id}")
        .master(os.environ.get("SPARK_MASTER_URL", "local[*]"))
        .config("spark.hadoop.fs.s3a.endpoint", minio_ep)
        .config("spark.hadoop.fs.s3a.access.key", ak)
        .config("spark.hadoop.fs.s3a.secret.key", sk)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    try:
        file_path = config.get("file_path", "")
        file_format = config.get("file_format", source_type)
        logger.info("Reading %s from %s", file_format, file_path)

        if file_format == "csv":
            df = spark.read.option("header", str(config.get("csv_header", True))) \
                          .option("inferSchema", "true") \
                          .option("sep", config.get("csv_delimiter", ",")) \
                          .csv(file_path)
        elif file_format == "json":
            df = spark.read.json(file_path)
        elif file_format == "parquet":
            df = spark.read.parquet(file_path)
        else:
            raise ValueError(f"Unsupported format: {file_format}")

        record_count = df.count()
        logger.info("Read %d records, writing to %s", record_count, bronze_path)

        df.write.mode("overwrite").option("header", "true").csv(bronze_path)

        ctx["ti"].xcom_push(key="bronze_path", value=bronze_path)
        ctx["ti"].xcom_push(key="record_count", value=record_count)
        logger.info("✅ Bronze ingestion complete: %d records", record_count)
    finally:
        spark.stop()


def log_summary(**ctx):
    ti = ctx["ti"]
    path = ti.xcom_pull(task_ids="ingest_bronze_data", key="bronze_path")
    count = ti.xcom_pull(task_ids="ingest_bronze_data", key="record_count")
    logger.info("=" * 60)
    logger.info("  Bronze Ingestion Summary")
    logger.info("  Records : %s", count)
    logger.info("  Path    : %s", path)
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="bronze_test_csv_flight_986ed019",
    default_args=default_args,
    description="Bronze ingestion from csv files — flight",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['auto-generated', 'bronze', 'flight'],
    max_active_runs=1,
) as dag:

    t_buckets = PythonOperator(task_id="create_buckets", python_callable=create_buckets)
    t_ingest  = PythonOperator(task_id="ingest_bronze_data", python_callable=ingest_bronze)
    t_summary = PythonOperator(task_id="log_summary", python_callable=log_summary)

    t_buckets >> t_ingest >> t_summary
