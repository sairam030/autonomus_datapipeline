"""
Utility to persist every piece of generated PySpark / DAG code
to a local `generated_queries/` directory for future reference
and debugging.

Directory layout
================
generated_queries/
├── <project_slug>/
│   ├── silver/
│   │   ├── 2026-02-15_10-30-00__add_category_column__ai_generated.py
│   │   ├── 2026-02-15_10-35-12__add_category_column__confirmed_v2.py
│   │   ├── 2026-02-15_10-40-00__add_category_column__dry_run.py
│   │   └── 2026-02-15_10-45-00__upload_to_silver__pipeline.py
│   ├── bronze/
│   │   ├── 2026-02-15_09-00-00__bronze_ingestion__v1.py
│   │   └── 2026-02-15_09-00-00__bronze_ingestion_config__v1.json
│   └── dags/
│       ├── 2026-02-15_09-30-00__bronze_dag.py
│       └── 2026-02-15_09-35-00__master_dag.py
"""

import logging
import os
import re
import json
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Root directory for all saved queries — mapped via Docker volume
GENERATED_QUERIES_DIR = os.environ.get(
    "GENERATED_QUERIES_DIR", "/opt/pipeline/generated_queries"
)


def _slugify(text: str, max_len: int = 60) -> str:
    """Convert text to a safe filename slug."""
    slug = re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")
    return slug[:max_len]


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")


def _write(filepath: str, content: str) -> str:
    """Write content and return the filepath."""
    _ensure_dir(os.path.dirname(filepath))
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
    logger.info("Saved generated code → %s", filepath)
    return filepath


# =========================================================================
# Header comment injected at the top of every saved file
# =========================================================================

def _header(
    kind: str,
    project_name: str,
    extra: dict | None = None,
) -> str:
    lines = [
        f"# ── Generated PySpark Code ──",
        f"# Kind:       {kind}",
        f"# Project:    {project_name}",
        f"# Saved at:   {datetime.now(timezone.utc).isoformat()}",
    ]
    if extra:
        for k, v in extra.items():
            lines.append(f"# {k}: {v}")
    lines.append(f"# {'─' * 60}\n")
    return "\n".join(lines)


# =========================================================================
# Public API
# =========================================================================

def save_silver_ai_generated(
    project_name: str,
    transform_name: str,
    user_query: str,
    code: str,
) -> str:
    """Save AI-generated silver transformation code."""
    slug = _slugify(project_name)
    ts = _timestamp()
    query_slug = _slugify(user_query, max_len=80)
    filename = f"{ts}__{query_slug}__ai_generated.py"
    filepath = os.path.join(GENERATED_QUERIES_DIR, slug, "silver", filename)
    header = _header(
        "Silver Transformation (AI Generated)",
        project_name,
        {"Transform": transform_name, "User Query": user_query[:200]},
    )
    return _write(filepath, header + code)


def save_silver_confirmed(
    project_name: str,
    transform_name: str,
    code: str,
    version: int = 1,
) -> str:
    """Save confirmed (user-approved) silver transformation code."""
    slug = _slugify(project_name)
    ts = _timestamp()
    name_slug = _slugify(transform_name)
    filename = f"{ts}__{name_slug}__confirmed_v{version}.py"
    filepath = os.path.join(GENERATED_QUERIES_DIR, slug, "silver", filename)
    header = _header(
        "Silver Transformation (Confirmed)",
        project_name,
        {"Transform": transform_name, "Version": str(version)},
    )
    return _write(filepath, header + code)


def save_silver_dry_run(
    project_name: str,
    transform_name: str,
    code: str,
) -> str:
    """Save code that was executed in a dry-run."""
    slug = _slugify(project_name)
    ts = _timestamp()
    name_slug = _slugify(transform_name)
    filename = f"{ts}__{name_slug}__dry_run.py"
    filepath = os.path.join(GENERATED_QUERIES_DIR, slug, "silver", filename)
    header = _header(
        "Silver Transformation (Dry Run)",
        project_name,
        {"Transform": transform_name},
    )
    return _write(filepath, header + code)


def save_silver_upload_pipeline(
    project_name: str,
    transforms: list[dict],
    bronze_path: str,
    silver_path: str,
) -> str:
    """
    Save the combined pipeline code used for the Upload-to-Silver execution.
    `transforms` should be a list of {"name": ..., "code": ...} dicts.
    """
    slug = _slugify(project_name)
    ts = _timestamp()
    filename = f"{ts}__upload_to_silver__pipeline.py"
    filepath = os.path.join(GENERATED_QUERIES_DIR, slug, "silver", filename)

    parts = [
        _header(
            "Silver Upload Pipeline",
            project_name,
            {
                "Bronze Path": bronze_path,
                "Silver Path": silver_path,
                "Transforms": str(len(transforms)),
            },
        ),
        "# This file contains all transformation functions applied sequentially",
        "# during the Upload-to-Silver execution.\n\n",
    ]

    for i, t in enumerate(transforms, 1):
        parts.append(f"# {'=' * 60}")
        parts.append(f"# Transform {i}: {t['name']} (v{t.get('version', 1)})")
        parts.append(f"# {'=' * 60}\n")
        parts.append(t["code"])
        parts.append("\n\n")

    return _write(filepath, "\n".join(parts))


def save_bronze_ingestion(
    project_name: str,
    pipeline_id: str,
    schema_version: int,
    file_format: str,
    compatible_files: list[str],
    fields: list[dict],
    bronze_path: str,
    csv_config: dict | None = None,
) -> str:
    """Save the bronze ingestion configuration and a reconstruction script."""
    slug = _slugify(project_name)
    ts = _timestamp()
    filename = f"{ts}__bronze_ingestion__v{schema_version}.py"
    filepath = os.path.join(GENERATED_QUERIES_DIR, slug, "bronze", filename)

    config = {
        "pipeline_id": pipeline_id,
        "project_name": project_name,
        "schema_version": schema_version,
        "file_format": file_format,
        "bronze_path": bronze_path,
        "compatible_files": compatible_files,
        "fields": fields,
        "csv_config": csv_config or {},
    }

    code = f'''\
{_header("Bronze Ingestion", project_name, {
    "Pipeline ID": pipeline_id,
    "Schema Version": str(schema_version),
    "File Format": file_format,
    "Bronze Path": bronze_path,
    "Files": str(len(compatible_files)),
})}
# ── Ingestion Configuration ──
import json

CONFIG = json.loads("""{json.dumps(config, indent=2, default=str)}""")


# ── Reconstruction Script ──
# Run this to replay the bronze ingestion manually if needed.
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("bronze_replay_{slug}_v{schema_version}")
    .getOrCreate()
)

# Read source files
bronze_path = CONFIG["bronze_path"]
files = CONFIG["compatible_files"]

for f in files:
    print(f"Reading: {{f}}")
    df = spark.read.option("header", "true").csv(f)
    print(f"  Rows: {{df.count()}}, Cols: {{len(df.columns)}}")

print(f"\\nBronze output would be written to: {{bronze_path}}")
'''
    return _write(filepath, code)


def save_dag_code(
    project_name: str,
    dag_type: str,
    dag_id: str,
    dag_code: str,
) -> str:
    """Save a generated Airflow DAG file."""
    slug = _slugify(project_name)
    ts = _timestamp()
    filename = f"{ts}__{dag_id}.py"
    filepath = os.path.join(GENERATED_QUERIES_DIR, slug, "dags", filename)
    header = _header(
        f"Airflow DAG ({dag_type})",
        project_name,
        {"DAG ID": dag_id, "DAG Type": dag_type},
    )
    return _write(filepath, header + dag_code)


def save_code_edit(
    project_name: str,
    transform_name: str,
    code: str,
) -> str:
    """Save manually edited transformation code."""
    slug = _slugify(project_name)
    ts = _timestamp()
    name_slug = _slugify(transform_name)
    filename = f"{ts}__{name_slug}__manual_edit.py"
    filepath = os.path.join(GENERATED_QUERIES_DIR, slug, "silver", filename)
    header = _header(
        "Silver Transformation (Manual Edit)",
        project_name,
        {"Transform": transform_name},
    )
    return _write(filepath, header + code)


# =========================================================================
# Gold Layer
# =========================================================================

def save_gold_ai_generated(
    project_name: str,
    transform_name: str,
    user_query: str,
    code: str,
) -> str:
    """Save AI-generated gold transformation code."""
    slug = _slugify(project_name)
    ts = _timestamp()
    query_slug = _slugify(user_query, max_len=80)
    filename = f"{ts}__{query_slug}__ai_generated.py"
    filepath = os.path.join(GENERATED_QUERIES_DIR, slug, "gold", filename)
    header = _header(
        "Gold Transformation (AI Generated)",
        project_name,
        {"Transform": transform_name, "User Query": user_query[:200]},
    )
    return _write(filepath, header + code)


def save_gold_confirmed(
    project_name: str,
    transform_name: str,
    code: str,
    version: int = 1,
) -> str:
    """Save confirmed (user-approved) gold transformation code."""
    slug = _slugify(project_name)
    ts = _timestamp()
    name_slug = _slugify(transform_name)
    filename = f"{ts}__{name_slug}__confirmed_v{version}.py"
    filepath = os.path.join(GENERATED_QUERIES_DIR, slug, "gold", filename)
    header = _header(
        "Gold Transformation (Confirmed)",
        project_name,
        {"Transform": transform_name, "Version": str(version)},
    )
    return _write(filepath, header + code)


def save_gold_dry_run(
    project_name: str,
    transform_name: str,
    code: str,
) -> str:
    """Save code that was executed in a gold dry-run."""
    slug = _slugify(project_name)
    ts = _timestamp()
    name_slug = _slugify(transform_name)
    filename = f"{ts}__{name_slug}__dry_run.py"
    filepath = os.path.join(GENERATED_QUERIES_DIR, slug, "gold", filename)
    header = _header(
        "Gold Transformation (Dry Run)",
        project_name,
        {"Transform": transform_name},
    )
    return _write(filepath, header + code)


def save_gold_upload_pipeline(
    project_name: str,
    transforms: list[dict],
    silver_path: str,
    gold_path: str,
) -> str:
    """Save the combined pipeline code used for the Upload-to-Gold execution."""
    slug = _slugify(project_name)
    ts = _timestamp()
    filename = f"{ts}__upload_to_gold__pipeline.py"
    filepath = os.path.join(GENERATED_QUERIES_DIR, slug, "gold", filename)

    parts = [
        _header(
            "Gold Upload Pipeline",
            project_name,
            {
                "Silver Path": silver_path,
                "Gold Path": gold_path,
                "Transforms": str(len(transforms)),
            },
        ),
        "# This file contains all transformation functions applied sequentially",
        "# during the Upload-to-Gold execution.\n\n",
    ]

    for i, t in enumerate(transforms, 1):
        parts.append(f"# {'=' * 60}")
        parts.append(f"# Transform {i}: {t['name']} (v{t.get('version', 1)})")
        parts.append(f"# {'=' * 60}\n")
        parts.append(t["code"])
        parts.append("\n\n")

    return _write(filepath, "\n".join(parts))


def save_gold_code_edit(
    project_name: str,
    transform_name: str,
    code: str,
) -> str:
    """Save manually edited gold transformation code."""
    slug = _slugify(project_name)
    ts = _timestamp()
    name_slug = _slugify(transform_name)
    filename = f"{ts}__{name_slug}__manual_edit.py"
    filepath = os.path.join(GENERATED_QUERIES_DIR, slug, "gold", filename)
    header = _header(
        "Gold Transformation (Manual Edit)",
        project_name,
        {"Transform": transform_name},
    )
    return _write(filepath, header + code)
