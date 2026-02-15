"""
Template-based Airflow DAG Generator

Generates Python DAG files from project/task configuration.
No LLM needed — deterministic template rendering.

Produces:
  - Individual DAGs per task (bronze_<project_id>, silver_<project_id>, etc.)
  - A master DAG that chains them via TriggerDagRunOperator
"""

import os
import re
import logging
from datetime import datetime
from typing import Optional
from uuid import UUID

logger = logging.getLogger(__name__)

# Where Airflow expects DAGs (inside the container)
DAGS_DIR = os.environ.get("DAGS_OUTPUT_DIR", "/opt/airflow/dags")


def _safe_dag_name(project_name: str, project_id: str) -> str:
    """Generate a filesystem/Airflow-safe DAG id from project name + id prefix."""
    slug = re.sub(r"[^a-z0-9]+", "_", project_name.lower()).strip("_")
    short_id = str(project_id)[:8]
    return f"{slug}_{short_id}"


# ============================================================================
# BRONZE DAG TEMPLATES
# ============================================================================

_BRONZE_FILE_TEMPLATE = '''\
"""
Auto-generated Bronze Ingestion DAG — File Source
Project : {project_name}
Source   : {source_type} files
Generated: {generated_at}
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os, json, logging

logger = logging.getLogger(__name__)

default_args = {{
    "owner": "{owner}",
    "depends_on_past": False,
    "retries": {retries},
    "retry_delay": timedelta(minutes={retry_delay_min}),
    "execution_timeout": timedelta(hours=2),
}}

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

    config = {source_config}
    pipeline_id = "{pipeline_id}"
    source_type = "{source_type}"

    minio_ep = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    ak = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    sk = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
    bronze_path = f"s3a://bronze/{{pipeline_id}}/{{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}}"

    spark = (
        SparkSession.builder
        .appName(f"bronze_ingest_{{pipeline_id}}")
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
            df = spark.read.option("header", str(config.get("csv_header", True))) \\
                          .option("inferSchema", "true") \\
                          .option("sep", config.get("csv_delimiter", ",")) \\
                          .csv(file_path)
        elif file_format == "json":
            df = spark.read.json(file_path)
        elif file_format == "parquet":
            df = spark.read.parquet(file_path)
        else:
            raise ValueError(f"Unsupported format: {{file_format}}")

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
    dag_id="{dag_id}",
    default_args=default_args,
    description="Bronze ingestion from {source_type} files — {project_name}",
    schedule_interval={schedule},
    start_date=datetime({start_year}, {start_month}, {start_day}),
    catchup=False,
    tags={tags},
    max_active_runs=1,
) as dag:

    t_buckets = PythonOperator(task_id="create_buckets", python_callable=create_buckets)
    t_ingest  = PythonOperator(task_id="ingest_bronze_data", python_callable=ingest_bronze)
    t_summary = PythonOperator(task_id="log_summary", python_callable=log_summary)

    t_buckets >> t_ingest >> t_summary
'''


_BRONZE_API_TEMPLATE = '''\
"""
Auto-generated Bronze Ingestion DAG — REST API Source
Project : {project_name}
Source   : REST API ({api_endpoint})
Generated: {generated_at}
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os, json, logging, requests

logger = logging.getLogger(__name__)

default_args = {{
    "owner": "{owner}",
    "depends_on_past": False,
    "retries": {retries},
    "retry_delay": timedelta(minutes={retry_delay_min}),
    "execution_timeout": timedelta(hours=2),
}}

# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def create_buckets(**ctx):
    from minio import Minio
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "").replace("https://", "")
    client = Minio(endpoint, access_key=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
                   secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"), secure=False)
    for bucket in ["bronze", "silver", "gold"]:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)


def fetch_api_data(**ctx):
    """Fetch data from REST API and write to MinIO Bronze layer."""
    import pandas as pd
    from io import BytesIO, StringIO

    api_config = {api_config}
    pipeline_id = "{pipeline_id}"

    endpoint = api_config["endpoint"]
    method = api_config.get("method", "GET").upper()
    headers = api_config.get("headers", {{}})
    body = api_config.get("body", {{}})
    auth_type = api_config.get("auth_type", "none")
    pagination = api_config.get("pagination", {{}})

    # Auth handling
    auth = None
    if auth_type == "basic":
        creds = api_config.get("credentials", {{}})
        auth = (creds.get("username", ""), creds.get("password", ""))
    elif auth_type == "bearer":
        creds = api_config.get("credentials", {{}})
        headers["Authorization"] = f"Bearer {{creds.get('token', '')}}"

    logger.info("Fetching from API: %s %s", method, endpoint)

    all_records = []
    url = endpoint
    page = 1
    max_pages = pagination.get("max_pages", 100)

    while url and page <= max_pages:
        if method == "GET":
            resp = requests.get(url, headers=headers, auth=auth, timeout=60)
        else:
            resp = requests.post(url, headers=headers, auth=auth, json=body, timeout=60)

        resp.raise_for_status()
        data = resp.json()

        # Extract records from response
        data_key = api_config.get("data_key", None)
        if data_key and isinstance(data, dict):
            records = data.get(data_key, [])
        elif isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = [data]
        else:
            records = [data]

        all_records.extend(records)
        logger.info("Page %d: fetched %d records (total: %d)", page, len(records), len(all_records))

        # Pagination
        next_key = pagination.get("next_key", None)
        if next_key and isinstance(data, dict):
            url = data.get(next_key)
        else:
            url = None
        page += 1

    if not all_records:
        logger.warning("No records fetched from API")
        return

    df = pd.DataFrame(all_records)
    logger.info("Total records fetched: %d, columns: %s", len(df), list(df.columns))

    # Write to MinIO
    from minio import Minio
    minio_ep = os.environ.get("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "").replace("https://", "")
    client = Minio(minio_ep, access_key=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
                   secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"), secure=False)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    object_name = f"{{pipeline_id}}/{{ts}}/data.csv"

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    client.put_object("bronze", object_name, BytesIO(csv_bytes), len(csv_bytes), content_type="text/csv")

    bronze_path = f"s3a://bronze/{{object_name}}"
    ctx["ti"].xcom_push(key="bronze_path", value=bronze_path)
    ctx["ti"].xcom_push(key="record_count", value=len(df))
    logger.info("✅ API Bronze ingestion complete: %d records → %s", len(df), bronze_path)


def log_summary(**ctx):
    ti = ctx["ti"]
    path = ti.xcom_pull(task_ids="fetch_api_data", key="bronze_path")
    count = ti.xcom_pull(task_ids="fetch_api_data", key="record_count")
    logger.info("=" * 60)
    logger.info("  API Bronze Summary — %s records → %s", count, path)
    logger.info("=" * 60)


with DAG(
    dag_id="{dag_id}",
    default_args=default_args,
    description="Bronze ingestion from REST API — {project_name}",
    schedule_interval={schedule},
    start_date=datetime({start_year}, {start_month}, {start_day}),
    catchup=False,
    tags={tags},
    max_active_runs=1,
) as dag:

    t_buckets = PythonOperator(task_id="create_buckets", python_callable=create_buckets)
    t_fetch   = PythonOperator(task_id="fetch_api_data", python_callable=fetch_api_data)
    t_summary = PythonOperator(task_id="log_summary", python_callable=log_summary)

    t_buckets >> t_fetch >> t_summary
'''


_BRONZE_KAFKA_TEMPLATE = '''\
"""
Auto-generated Bronze Ingestion DAG — Kafka Source
Project : {project_name}
Source   : Kafka topic ({kafka_topic})
Generated: {generated_at}
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os, json, logging

logger = logging.getLogger(__name__)

default_args = {{
    "owner": "{owner}",
    "depends_on_past": False,
    "retries": {retries},
    "retry_delay": timedelta(minutes={retry_delay_min}),
    "execution_timeout": timedelta(hours=2),
}}

# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def create_buckets(**ctx):
    from minio import Minio
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "").replace("https://", "")
    client = Minio(endpoint, access_key=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
                   secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"), secure=False)
    for bucket in ["bronze", "silver", "gold"]:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)


def consume_kafka(**ctx):
    """Consume messages from Kafka and write to MinIO Bronze layer."""
    import pandas as pd
    from io import BytesIO
    from kafka import KafkaConsumer

    kafka_config = {kafka_config}
    pipeline_id = "{pipeline_id}"

    bootstrap = kafka_config["bootstrap_servers"]
    topic = kafka_config["topic"]
    group_id = kafka_config.get("group_id", f"bronze_{{pipeline_id}}")
    max_messages = kafka_config.get("max_messages", 10000)
    timeout_ms = kafka_config.get("consumer_timeout_ms", 30000)
    value_deserializer = kafka_config.get("deserializer", "json")

    logger.info("Consuming from Kafka: %s topic=%s group=%s", bootstrap, topic, group_id)

    if value_deserializer == "json":
        deser = lambda v: json.loads(v.decode("utf-8"))
    else:
        deser = lambda v: {{"value": v.decode("utf-8", errors="replace")}}

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap.split(","),
        group_id=group_id,
        auto_offset_reset=kafka_config.get("auto_offset_reset", "earliest"),
        enable_auto_commit=True,
        consumer_timeout_ms=timeout_ms,
        value_deserializer=deser,
    )

    records = []
    for msg in consumer:
        record = msg.value if isinstance(msg.value, dict) else {{"value": msg.value}}
        record["_kafka_topic"] = msg.topic
        record["_kafka_partition"] = msg.partition
        record["_kafka_offset"] = msg.offset
        record["_kafka_timestamp"] = msg.timestamp
        records.append(record)
        if len(records) >= max_messages:
            break

    consumer.close()
    logger.info("Consumed %d messages from topic %s", len(records), topic)

    if not records:
        logger.warning("No messages consumed")
        ctx["ti"].xcom_push(key="record_count", value=0)
        return

    df = pd.DataFrame(records)

    # Write to MinIO
    from minio import Minio
    minio_ep = os.environ.get("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "").replace("https://", "")
    client = Minio(minio_ep, access_key=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
                   secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"), secure=False)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    object_name = f"{{pipeline_id}}/{{ts}}/data.csv"

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    client.put_object("bronze", object_name, BytesIO(csv_bytes), len(csv_bytes), content_type="text/csv")

    bronze_path = f"s3a://bronze/{{object_name}}"
    ctx["ti"].xcom_push(key="bronze_path", value=bronze_path)
    ctx["ti"].xcom_push(key="record_count", value=len(df))
    logger.info("✅ Kafka Bronze ingestion complete: %d records → %s", len(df), bronze_path)


def log_summary(**ctx):
    ti = ctx["ti"]
    path = ti.xcom_pull(task_ids="consume_kafka", key="bronze_path")
    count = ti.xcom_pull(task_ids="consume_kafka", key="record_count")
    logger.info("=" * 60)
    logger.info("  Kafka Bronze Summary — %s records → %s", count, path)
    logger.info("=" * 60)


with DAG(
    dag_id="{dag_id}",
    default_args=default_args,
    description="Bronze ingestion from Kafka — {project_name}",
    schedule_interval={schedule},
    start_date=datetime({start_year}, {start_month}, {start_day}),
    catchup=False,
    tags={tags},
    max_active_runs=1,
) as dag:

    t_buckets = PythonOperator(task_id="create_buckets", python_callable=create_buckets)
    t_consume = PythonOperator(task_id="consume_kafka", python_callable=consume_kafka)
    t_summary = PythonOperator(task_id="log_summary", python_callable=log_summary)

    t_buckets >> t_consume >> t_summary
'''


# ============================================================================
# MASTER DAG TEMPLATE (chains all tasks via TriggerDagRunOperator)
# ============================================================================

_MASTER_DAG_TEMPLATE = '''\
"""
Auto-generated Master Pipeline DAG
Project : {project_name}
Chains  : {task_dag_ids}
Generated: {generated_at}
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
import logging

logger = logging.getLogger(__name__)

default_args = {{
    "owner": "{owner}",
    "depends_on_past": False,
    "retries": {retries},
    "retry_delay": timedelta(minutes={retry_delay_min}),
}}


def start_pipeline(**ctx):
    logger.info("🚀 Starting master pipeline: {project_name}")


def pipeline_complete(**ctx):
    logger.info("✅ Master pipeline complete: {project_name}")


with DAG(
    dag_id="{dag_id}",
    default_args=default_args,
    description="Master pipeline — {project_name}",
    schedule_interval={schedule},
    start_date=datetime({start_year}, {start_month}, {start_day}),
    catchup=False,
    tags={tags},
    max_active_runs=1,
) as dag:

    t_start = PythonOperator(task_id="start_pipeline", python_callable=start_pipeline)
    t_done  = PythonOperator(task_id="pipeline_complete", python_callable=pipeline_complete)

{trigger_tasks}

    # Chain: start → bronze → silver → gold → done
{chain_expr}
'''


# ============================================================================
# Generator class
# ============================================================================

class DAGGenerator:
    """
    Generates Airflow DAG .py files from project configuration.
    """

    def __init__(self, dags_dir: str = DAGS_DIR):
        self.dags_dir = dags_dir
        os.makedirs(self.dags_dir, exist_ok=True)

    # ------------------------------------------------------------------
    # Bronze DAG
    # ------------------------------------------------------------------
    def generate_bronze_dag(
        self,
        project_name: str,
        project_id: str,
        source_type: str,
        source_config: dict,
        schedule: Optional[str] = None,
        start_date: Optional[datetime] = None,
        retries: int = 1,
        retry_delay_min: int = 5,
        owner: str = "autonomous-pipeline",
        task_label: Optional[str] = None,
    ) -> dict:
        """Generate a Bronze DAG file. Returns metadata about the generated DAG."""

        dag_base = _safe_dag_name(project_name, project_id)
        if task_label:
            label_slug = re.sub(r"[^a-z0-9]+", "_", task_label.lower()).strip("_")
            dag_id = f"bronze_{label_slug}_{dag_base}"
        else:
            dag_id = f"bronze_{dag_base}"
        start = start_date or datetime(2024, 1, 1)
        schedule_str = f'"{schedule}"' if schedule else "None"
        tags_str = str(["auto-generated", "bronze", project_name[:30]])
        generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

        common_vars = dict(
            project_name=project_name,
            pipeline_id=str(project_id),
            dag_id=dag_id,
            owner=owner,
            retries=retries,
            retry_delay_min=retry_delay_min,
            schedule=schedule_str,
            start_year=start.year,
            start_month=start.month,
            start_day=start.day,
            tags=tags_str,
            generated_at=generated_at,
        )

        if source_type in ("csv", "json", "parquet"):
            template = _BRONZE_FILE_TEMPLATE
            common_vars["source_type"] = source_type
            common_vars["source_config"] = repr(source_config)
        elif source_type == "api":
            template = _BRONZE_API_TEMPLATE
            common_vars["api_endpoint"] = source_config.get("endpoint", "")
            common_vars["api_config"] = repr(source_config)
        elif source_type == "kafka":
            template = _BRONZE_KAFKA_TEMPLATE
            common_vars["kafka_topic"] = source_config.get("topic", "")
            common_vars["kafka_config"] = repr(source_config)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

        dag_code = template.format(**common_vars)

        filename = f"{dag_id}.py"
        filepath = os.path.join(self.dags_dir, filename)
        with open(filepath, "w") as f:
            f.write(dag_code)

        logger.info("Generated Bronze DAG: %s → %s", dag_id, filepath)

        return {
            "dag_id": dag_id,
            "dag_type": "bronze",
            "source_type": source_type,
            "filename": filename,
            "filepath": filepath,
            "schedule": schedule,
        }

    # ------------------------------------------------------------------
    # Master DAG
    # ------------------------------------------------------------------
    def generate_master_dag(
        self,
        project_name: str,
        project_id: str,
        task_dag_ids: list[str],
        schedule: Optional[str] = None,
        start_date: Optional[datetime] = None,
        retries: int = 1,
        retry_delay_min: int = 5,
        owner: str = "autonomous-pipeline",
    ) -> dict:
        """Generate a Master DAG that chains individual task DAGs."""

        dag_base = _safe_dag_name(project_name, project_id)
        dag_id = f"master_{dag_base}"
        start = start_date or datetime(2024, 1, 1)
        schedule_str = f'"{schedule}"' if schedule else "None"
        tags_str = str(["auto-generated", "master", project_name[:30]])
        generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

        # Build trigger tasks
        trigger_lines = []
        chain_parts = ["t_start"]
        for i, tid in enumerate(task_dag_ids):
            var_name = f"t_trigger_{i}"
            trigger_lines.append(
                f'    {var_name} = TriggerDagRunOperator(\n'
                f'        task_id="trigger_{tid}",\n'
                f'        trigger_dag_id="{tid}",\n'
                f'        wait_for_completion=True,\n'
                f'        poke_interval=30,\n'
                f'    )'
            )
            chain_parts.append(var_name)
        chain_parts.append("t_done")

        trigger_block = "\n\n".join(trigger_lines)
        chain_expr = "    " + " >> ".join(chain_parts)

        dag_code = _MASTER_DAG_TEMPLATE.format(
            project_name=project_name,
            dag_id=dag_id,
            task_dag_ids=", ".join(task_dag_ids),
            owner=owner,
            retries=retries,
            retry_delay_min=retry_delay_min,
            schedule=schedule_str,
            start_year=start.year,
            start_month=start.month,
            start_day=start.day,
            tags=tags_str,
            generated_at=generated_at,
            trigger_tasks=trigger_block,
            chain_expr=chain_expr,
        )

        filename = f"{dag_id}.py"
        filepath = os.path.join(self.dags_dir, filename)
        with open(filepath, "w") as f:
            f.write(dag_code)

        logger.info("Generated Master DAG: %s → %s", dag_id, filepath)

        return {
            "dag_id": dag_id,
            "dag_type": "master",
            "child_dags": task_dag_ids,
            "filename": filename,
            "filepath": filepath,
            "schedule": schedule,
        }

    # ------------------------------------------------------------------
    # Full pipeline generation (convenience)
    # ------------------------------------------------------------------
    def generate_full_pipeline(
        self,
        project_name: str,
        project_id: str,
        source_type: str,
        source_config: dict,
        schedule: Optional[str] = None,
        start_date: Optional[datetime] = None,
        retries: int = 1,
        retry_delay_min: int = 5,
        owner: str = "autonomous-pipeline",
    ) -> dict:
        """
        Generate individual DAGs + master DAG for a complete pipeline.
        Returns metadata about all generated DAGs.
        """
        results = {"dags": [], "master": None}

        # Bronze
        bronze = self.generate_bronze_dag(
            project_name=project_name,
            project_id=project_id,
            source_type=source_type,
            source_config=source_config,
            schedule=schedule,
            start_date=start_date,
            retries=retries,
            retry_delay_min=retry_delay_min,
            owner=owner,
        )
        results["dags"].append(bronze)

        # Master DAG chains all individual DAGs
        task_ids = [d["dag_id"] for d in results["dags"]]
        master = self.generate_master_dag(
            project_name=project_name,
            project_id=project_id,
            task_dag_ids=task_ids,
            schedule=schedule,
            start_date=start_date,
            retries=retries,
            retry_delay_min=retry_delay_min,
            owner=owner,
        )
        results["master"] = master

        return results

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------
    def list_generated_dags(self, project_id: Optional[str] = None) -> list[dict]:
        """List all generated DAG files, optionally filtered by project."""
        dags = []
        short_id = str(project_id)[:8] if project_id else None
        for f in os.listdir(self.dags_dir):
            if not f.endswith(".py"):
                continue
            if short_id and short_id not in f:
                continue
            filepath = os.path.join(self.dags_dir, f)
            dags.append({
                "filename": f,
                "filepath": filepath,
                "size_bytes": os.path.getsize(filepath),
                "modified_at": datetime.fromtimestamp(os.path.getmtime(filepath)).isoformat(),
            })
        return dags

    def delete_dag(self, filename: str) -> bool:
        """Delete a generated DAG file."""
        filepath = os.path.join(self.dags_dir, filename)
        if os.path.exists(filepath):
            os.remove(filepath)
            logger.info("Deleted DAG file: %s", filepath)
            return True
        return False

    def delete_project_dags(self, project_id: str) -> int:
        """Delete all DAGs for a project. Returns number deleted."""
        short_id = str(project_id)[:8]
        count = 0
        for f in os.listdir(self.dags_dir):
            if f.endswith(".py") and short_id in f:
                os.remove(os.path.join(self.dags_dir, f))
                count += 1
        logger.info("Deleted %d DAG files for project %s", count, project_id)
        return count
