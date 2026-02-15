"""
Sandbox — Safe execution environment for user/AI-generated PySpark code.

Provides:
  - _SAFE_BUILTINS: restricted builtins dict (no file I/O, no eval/exec)
  - safe_import():  only allows data-processing libraries
  - build_safe_exec_globals(): builds the sandboxed globals dict
  - execute_dry_run(): runs transform code on sample data in a local Spark session
  - build_sample_rows_from_schema(): synthesizes sample rows from schema metadata

These are used by both Silver and Gold routers for chat dry-runs and uploads.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)

# ============================================================================
# Allowed builtins — no file I/O, no eval/exec, no import manipulation
# ============================================================================

_BLOCKED_BUILTINS = {
    'eval', 'exec', 'compile', '__import__', 'open',
    'breakpoint', 'exit', 'quit', 'input',
    'globals', 'locals', 'vars', 'dir',
    'getattr', 'setattr', 'delattr',
    'memoryview', 'classmethod', 'staticmethod',
}

SAFE_BUILTINS: dict[str, Any] = (
    {
        k: v for k, v in __builtins__.items()  # type: ignore[union-attr]
        if k not in _BLOCKED_BUILTINS
    }
    if isinstance(__builtins__, dict)
    else {
        k: getattr(__builtins__, k)
        for k in dir(__builtins__)
        if not k.startswith('_') and k not in _BLOCKED_BUILTINS
    }
)

# ============================================================================
# Safe import — whitelist of data-processing modules
# ============================================================================

_ALLOWED_MODULES = {
    'pyspark', 'pyspark.sql', 'pyspark.sql.functions',
    'pyspark.sql.types', 'pyspark.sql.window',
    'math', 'datetime', 'decimal', 'json', 're',
    'collections', 'functools', 'itertools', 'operator',
    'typing', 'string', 'hashlib', 'uuid',
}

_ALLOWED_TOP_LEVELS = {m.split('.')[0] for m in _ALLOWED_MODULES}


def safe_import(name: str, *args, **kwargs):
    """Only allow importing whitelisted data-processing libraries."""
    top_level = name.split('.')[0]
    if top_level not in _ALLOWED_TOP_LEVELS and name not in _ALLOWED_MODULES:
        raise ImportError(f"Import of '{name}' is not allowed in transformation code.")
    if isinstance(__builtins__, dict):
        return __builtins__['__import__'](name, *args, **kwargs)
    return __import__(name, *args, **kwargs)


def build_safe_exec_globals() -> dict:
    """Build a restricted globals dict for exec() to sandbox user/AI code."""
    return {'__builtins__': {**SAFE_BUILTINS, '__import__': safe_import}}


# ============================================================================
# Sample row synthesis from schema metadata
# ============================================================================

_TYPE_CASTERS = {
    "integer": lambda v: int(v) if v is not None else None,
    "long":    lambda v: int(v) if v is not None else None,
    "float":   lambda v: float(v) if v is not None else None,
    "double":  lambda v: float(v) if v is not None else None,
    "boolean": lambda v: (str(v).lower() in ("true", "1", "yes")) if v is not None else None,
    "string":  lambda v: str(v) if v is not None else None,
}


def build_sample_rows_from_schema(input_schema: list, num_rows: int = 10) -> list:
    """
    Synthesize sample rows from schema's sample_values.

    Each field in input_schema should have:
      - name, detected_type (or type), nullable, sample_values
    """
    if not input_schema:
        return []
    rows = []
    for i in range(num_rows):
        row: dict[str, Any] = {}
        for field in input_schema:
            name = field.get("name", "col")
            dtype = field.get("detected_type", field.get("type", "string")).lower()
            samples = field.get("sample_values", [])
            nullable = field.get("nullable", True)
            if samples:
                raw = samples[i % len(samples)]
            elif nullable:
                raw = None
            else:
                raw = "" if dtype == "string" else 0
            caster = _TYPE_CASTERS.get(dtype, _TYPE_CASTERS["string"])
            try:
                row[name] = caster(raw)
            except (ValueError, TypeError):
                row[name] = raw
        rows.append(row)
    return rows


# ============================================================================
# Dry-run — execute transform code on sample data in a local Spark session
# ============================================================================

def execute_dry_run(code: str, input_schema: list, sample_rows: list) -> dict:
    """
    Execute transform code on sample data using a local PySpark session.

    The code must define `def transform(df, spark):` which receives
    the sample DataFrame and returns the transformed DataFrame.

    Returns a dict with keys:
      success, output_rows, output_schema, row_count, error, validation_message
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType,
        FloatType, BooleanType, LongType,
    )

    TYPE_MAP = {
        "string": StringType(), "integer": IntegerType(), "long": LongType(),
        "float": FloatType(), "double": FloatType(), "boolean": BooleanType(),
    }

    spark = None
    try:
        # Stop any existing session to avoid config bleed
        try:
            existing = SparkSession.getActiveSession()
            if existing:
                existing.stop()
        except Exception:
            pass

        spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("dry_run")
            .config("spark.driver.memory", "512m")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

        # Build schema from field definitions
        fields = []
        for f in input_schema:
            name = f.get("name", "col")
            dtype = f.get("detected_type", f.get("type", "string")).lower()
            spark_type = TYPE_MAP.get(dtype, StringType())
            nullable = f.get("nullable", True)
            fields.append(StructField(name, spark_type, nullable))
        schema = StructType(fields) if fields else None

        # Create DataFrame from sample rows
        if sample_rows:
            rows = sample_rows[:10]
            df = spark.createDataFrame(rows, schema=schema) if schema else spark.createDataFrame(rows)
        elif schema:
            df = spark.createDataFrame([], schema=schema)
        else:
            return {
                "success": False, "output_rows": [], "output_schema": [],
                "row_count": 0, "error": "No sample data and no schema.",
                "validation_message": "Cannot create test DataFrame.",
            }

        # Execute in sandbox
        exec_globals = build_safe_exec_globals()
        exec(code, exec_globals)
        transform_fn = exec_globals.get("transform")
        if not transform_fn:
            return {
                "success": False, "output_rows": [], "output_schema": [],
                "row_count": 0, "error": "`transform` function not found.",
                "validation_message": "Code must define `def transform(df, spark):`",
            }

        result_df = transform_fn(df, spark)
        output_rows = [row.asDict() for row in result_df.collect()]
        output_schema = [
            {"name": f.name, "type": str(f.dataType), "nullable": f.nullable}
            for f in result_df.schema.fields
        ]

        return {
            "success": True, "output_rows": output_rows, "output_schema": output_schema,
            "row_count": len(output_rows), "error": None,
            "validation_message": f"Dry-run successful: {len(output_rows)} rows, {len(output_schema)} columns.",
        }

    except Exception as e:
        return {
            "success": False, "output_rows": [], "output_schema": [],
            "row_count": 0, "error": str(e),
            "validation_message": f"Dry-run failed: {str(e)}",
        }
    finally:
        if spark:
            spark.stop()
