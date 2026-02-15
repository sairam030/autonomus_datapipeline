"""
Schema Detection Service

Scans a directory of files (CSV, JSON, Parquet), detects schema,
validates all files against the detected schema, and flags mismatches.

Flow:
1. User provides directory path + file format
2. Service scans directory for all matching files
3. Reads first file (or sample) to detect schema
4. Validates ALL other files against detected schema
5. Returns: detected schema + compatible files + incompatible files with reasons
"""

import os
import re
import glob
import logging
from datetime import datetime, timezone
from typing import Optional, Any
from uuid import UUID

import pandas as pd
import requests

from backend.app.schemas.schema import (
    FieldSchema,
    FileInfo,
    SchemaDetectionResult,
)
from backend.app.config import get_settings

logger = logging.getLogger(__name__)

# =============================================================================
# Type detection heuristics
# =============================================================================

# Patterns for type inference
DATE_PATTERNS = [
    r"^\d{4}-\d{2}-\d{2}$",                       # 2024-01-15
    r"^\d{2}/\d{2}/\d{4}$",                        # 01/15/2024
    r"^\d{2}-\d{2}-\d{4}$",                        # 15-01-2024
]

DATETIME_PATTERNS = [
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}",         # 2024-01-15T10:30:00
    r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}",             # 01/15/2024 10:30
]

BOOLEAN_VALUES = {"true", "false", "yes", "no", "1", "0", "t", "f", "y", "n"}

# Pandas type → our type mapping
PANDAS_TYPE_MAP = {
    "int64": "integer",
    "int32": "integer",
    "float64": "float",
    "float32": "float",
    "bool": "boolean",
    "datetime64[ns]": "timestamp",
    "datetime64[ns, UTC]": "timestamp",
    "object": "string",       # Will be further analyzed
    "category": "string",
}


def _infer_string_column_type(series: pd.Series) -> tuple[str, float]:
    """
    For a pandas 'object' (string) column, try to detect the actual type
    by sampling values. Returns (detected_type, confidence).
    """
    non_null = series.dropna()
    if len(non_null) == 0:
        return "string", 0.5

    sample = non_null.head(min(200, len(non_null)))
    sample_str = sample.astype(str)

    # Try boolean
    bool_matches = sum(1 for v in sample_str if v.strip().lower() in BOOLEAN_VALUES)
    if bool_matches / len(sample_str) > 0.95:
        return "boolean", bool_matches / len(sample_str)

    # Try integer
    int_matches = 0
    for v in sample_str:
        try:
            val = v.strip().replace(",", "")
            if val == "":
                continue
            int(val)
            int_matches += 1
        except (ValueError, TypeError):
            pass
    if int_matches / len(sample_str) > 0.95:
        return "integer", int_matches / len(sample_str)

    # Try float
    float_matches = 0
    for v in sample_str:
        try:
            val = v.strip().replace(",", "")
            if val == "":
                continue
            float(val)
            float_matches += 1
        except (ValueError, TypeError):
            pass
    if float_matches / len(sample_str) > 0.95:
        return "float", float_matches / len(sample_str)

    # Try datetime
    dt_matches = sum(
        1 for v in sample_str
        if any(re.match(p, v.strip()) for p in DATETIME_PATTERNS)
    )
    if dt_matches / len(sample_str) > 0.8:
        return "timestamp", dt_matches / len(sample_str)

    # Try date
    date_matches = sum(
        1 for v in sample_str
        if any(re.match(p, v.strip()) for p in DATE_PATTERNS)
    )
    if date_matches / len(sample_str) > 0.8:
        return "date", date_matches / len(sample_str)

    return "string", 1.0


def _analyze_field(name: str, series: pd.Series, total_rows: int) -> FieldSchema:
    """Analyze a single column and produce a FieldSchema."""
    dtype_str = str(series.dtype)
    null_count = int(series.isnull().sum())
    non_null = series.dropna()

    # nunique / unique can fail on columns containing lists or dicts
    try:
        unique_count = int(non_null.nunique()) if len(non_null) > 0 else 0
    except TypeError:
        unique_count = 0

    # Map pandas type
    base_type = PANDAS_TYPE_MAP.get(dtype_str, "string")
    confidence = 1.0

    # If object type, do deeper analysis
    if base_type == "string" and len(non_null) > 0:
        # Check if values are complex types (list/dict) — skip string inference
        first_val = non_null.iloc[0]
        if isinstance(first_val, (list, dict)):
            base_type = "string"
            confidence = 0.8
        else:
            base_type, confidence = _infer_string_column_type(series)

    # Sample values (up to 5 unique non-null)
    sample_values = []
    if len(non_null) > 0:
        try:
            uniques = non_null.unique()[:5]
        except TypeError:
            # Fallback for unhashable types (lists, dicts)
            uniques = non_null.head(5).values
        sample_values = [_safe_serialize(v) for v in uniques]

    # Min/Max for numeric types
    min_val = None
    max_val = None
    if base_type in ("integer", "float") and len(non_null) > 0:
        try:
            numeric = pd.to_numeric(non_null, errors="coerce").dropna()
            if len(numeric) > 0:
                min_val = _safe_serialize(numeric.min())
                max_val = _safe_serialize(numeric.max())
        except Exception:
            pass

    return FieldSchema(
        name=name,
        detected_type=base_type,
        nullable=null_count > 0,
        confidence=round(confidence, 3),
        sample_values=sample_values,
        unique_count=unique_count,
        null_count=null_count,
        total_count=total_rows,
        min_value=min_val,
        max_value=max_val,
    )


def _safe_serialize(val) -> str:
    """Convert any value to a JSON-safe string representation."""
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
    except (ValueError, TypeError):
        pass  # val is a list/array — pd.isna returns array, not scalar
    if isinstance(val, (int, float, bool, str)):
        return val
    return str(val)


# =============================================================================
# File reading functions
# =============================================================================

def _read_file_sample(
    file_path: str,
    file_format: str,
    sample_rows: int = 1000,
    csv_delimiter: str = ",",
    csv_header: bool = True,
    csv_encoding: str = "utf-8",
) -> Optional[pd.DataFrame]:
    """Read a sample of rows from a file. Returns None on failure."""
    try:
        if file_format == "csv":
            df = pd.read_csv(
                file_path,
                nrows=sample_rows,
                sep=csv_delimiter,
                header=0 if csv_header else None,
                encoding=csv_encoding,
                low_memory=False,
            )
        elif file_format == "json":
            df = pd.read_json(file_path, lines=True, nrows=sample_rows)
        elif file_format == "parquet":
            df = pd.read_parquet(file_path)
            if len(df) > sample_rows:
                df = df.head(sample_rows)
        else:
            logger.error(f"Unsupported format: {file_format}")
            return None

        return df
    except Exception as e:
        logger.error(f"Failed to read {file_path}: {e}")
        return None


def _get_file_row_count(
    file_path: str,
    file_format: str,
    csv_delimiter: str = ",",
    csv_header: bool = True,
    csv_encoding: str = "utf-8",
) -> Optional[int]:
    """Get total row count for a file. Returns None on failure."""
    try:
        if file_format == "csv":
            # Fast line count
            with open(file_path, "r", encoding=csv_encoding) as f:
                count = sum(1 for _ in f)
            return count - (1 if csv_header else 0)
        elif file_format == "json":
            with open(file_path, "r") as f:
                count = sum(1 for _ in f)
            return count
        elif file_format == "parquet":
            import pyarrow.parquet as pq
            pf = pq.ParquetFile(file_path)
            return pf.metadata.num_rows
        return None
    except Exception:
        return None


def _get_file_schema_signature(
    file_path: str,
    file_format: str,
    csv_delimiter: str = ",",
    csv_header: bool = True,
    csv_encoding: str = "utf-8",
) -> Optional[tuple]:
    """
    Get a lightweight schema signature (column names + dtypes) for a file
    without reading the full file. Used for compatibility checking.
    """
    try:
        if file_format == "csv":
            df = pd.read_csv(
                file_path, nrows=5, sep=csv_delimiter,
                header=0 if csv_header else None,
                encoding=csv_encoding, low_memory=False,
            )
            return tuple(sorted(df.columns.tolist()))
        elif file_format == "json":
            df = pd.read_json(file_path, lines=True, nrows=5)
            return tuple(sorted(df.columns.tolist()))
        elif file_format == "parquet":
            import pyarrow.parquet as pq
            schema = pq.read_schema(file_path)
            return tuple(sorted(schema.names))
        return None
    except Exception as e:
        logger.error(f"Failed to get schema for {file_path}: {e}")
        return None


# =============================================================================
# Main detection function
# =============================================================================

def scan_and_detect_schema(
    directory_path: str,
    file_format: str,
    pipeline_id: UUID,
    csv_delimiter: str = ",",
    csv_header: bool = True,
    csv_encoding: str = "utf-8",
    sample_rows: int = 1000,
) -> SchemaDetectionResult:
    """
    Main entry point: scan a directory, detect schema, validate all files.

    Steps:
    1. Find all files matching the format in the directory
    2. Read sample from the first file to detect schema
    3. Check all other files against the detected schema
    4. Return complete detection result

    Args:
        directory_path: Absolute path to directory containing data files
        file_format: csv, json, or parquet
        pipeline_id: UUID of the pipeline
        csv_delimiter: Delimiter for CSV files
        csv_header: Whether CSV files have a header row
        csv_encoding: Encoding for CSV files
        sample_rows: Number of rows to sample for detection

    Returns:
        SchemaDetectionResult with fields, compatible/incompatible files
    """
    settings = get_settings()

    # -------------------------------------------------------------------------
    # Step 1: Find all files
    # -------------------------------------------------------------------------
    extension_map = {
        "csv": ["*.csv", "*.CSV"],
        "json": ["*.json", "*.JSON", "*.jsonl", "*.JSONL"],
        "parquet": ["*.parquet", "*.PARQUET", "*.pq"],
    }

    patterns = extension_map.get(file_format, [f"*.{file_format}"])
    all_files = []
    for pattern in patterns:
        all_files.extend(glob.glob(os.path.join(directory_path, pattern)))
        # Also search subdirectories (one level deep)
        all_files.extend(glob.glob(os.path.join(directory_path, "**", pattern), recursive=True))

    # Deduplicate and sort
    all_files = sorted(set(all_files))

    if not all_files:
        raise FileNotFoundError(
            f"No {file_format} files found in '{directory_path}'. "
            f"Searched patterns: {patterns}"
        )

    logger.info(f"Found {len(all_files)} {file_format} file(s) in {directory_path}")

    # -------------------------------------------------------------------------
    # Step 2: Read first file and detect schema
    # -------------------------------------------------------------------------
    primary_file = all_files[0]
    sample_df = _read_file_sample(
        primary_file, file_format, sample_rows,
        csv_delimiter, csv_header, csv_encoding,
    )

    if sample_df is None or sample_df.empty:
        raise ValueError(f"Could not read sample from {primary_file}")

    # Detect schema from sample
    fields: list[FieldSchema] = []
    for col in sample_df.columns:
        field = _analyze_field(col, sample_df[col], len(sample_df))
        fields.append(field)

    # Get reference schema signature (sorted column names)
    ref_signature = tuple(sorted(sample_df.columns.tolist()))

    logger.info(
        f"Detected {len(fields)} fields from {primary_file} "
        f"({len(sample_df)} sample rows)"
    )

    # -------------------------------------------------------------------------
    # Step 3: Validate all files against detected schema
    # -------------------------------------------------------------------------
    compatible_files: list[FileInfo] = []
    incompatible_files: list[FileInfo] = []

    for fpath in all_files:
        file_size = os.path.getsize(fpath)
        file_info_base = {
            "path": fpath,
            "filename": os.path.basename(fpath),
            "size_bytes": file_size,
        }

        # Get this file's schema signature
        file_signature = _get_file_schema_signature(
            fpath, file_format, csv_delimiter, csv_header, csv_encoding,
        )

        if file_signature is None:
            incompatible_files.append(FileInfo(
                **file_info_base,
                schema_match=False,
                mismatch_reason="Could not read file or detect schema",
            ))
            continue

        # Compare column sets
        ref_set = set(ref_signature)
        file_set = set(file_signature)

        missing_cols = ref_set - file_set
        extra_cols = file_set - ref_set

        if missing_cols or extra_cols:
            reasons = []
            if missing_cols:
                reasons.append(f"Missing columns: {sorted(missing_cols)}")
            if extra_cols:
                reasons.append(f"Extra columns: {sorted(extra_cols)}")

            incompatible_files.append(FileInfo(
                **file_info_base,
                schema_match=False,
                mismatch_reason="; ".join(reasons),
            ))
        else:
            # Compatible — get row count
            row_count = _get_file_row_count(
                fpath, file_format, csv_delimiter, csv_header, csv_encoding,
            )
            compatible_files.append(FileInfo(
                **file_info_base,
                row_count=row_count,
                schema_match=True,
            ))

    # -------------------------------------------------------------------------
    # Step 4: Calculate overall confidence
    # -------------------------------------------------------------------------
    field_confidences = [f.confidence for f in fields]
    avg_confidence = sum(field_confidences) / len(field_confidences) if field_confidences else 0.0
    file_ratio = len(compatible_files) / len(all_files) if all_files else 0.0
    overall_confidence = round((avg_confidence * 0.7 + file_ratio * 0.3), 3)

    logger.info(
        f"Schema detection complete: {len(compatible_files)} compatible, "
        f"{len(incompatible_files)} incompatible, confidence={overall_confidence}"
    )

    return SchemaDetectionResult(
        pipeline_id=pipeline_id,
        schema_version=1,
        fields=fields,
        total_files=len(all_files),
        compatible_files=compatible_files,
        incompatible_files=incompatible_files,
        sample_row_count=len(sample_df),
        detection_confidence=overall_confidence,
        detected_at=datetime.now(timezone.utc),
    )


# =============================================================================
# API schema detection
# =============================================================================

def _resolve_data_key(data: Any, data_key: str) -> list[dict]:
    """
    Traverse nested JSON using a data key like 'items[0].articles' or 'items.0.articles'.
    Supports both bracket notation (items[0]) and dot notation (items.0).
    Numeric segments index into arrays, string segments index into dicts.
    Returns the extracted list of records.
    """
    # Normalize bracket notation → dot notation:  items[0].articles → items.0.articles
    normalized = re.sub(r'\[(\d+)\]', r'.\1', data_key).strip('.')

    current = data
    for part in normalized.split("."):
        if current is None:
            raise ValueError(f"Cannot resolve data_key '{data_key}': hit None at '{part}'")
        if isinstance(current, list):
            try:
                idx = int(part)
                current = current[idx]
            except (ValueError, IndexError) as e:
                raise ValueError(
                    f"Cannot resolve data_key '{data_key}': "
                    f"expected numeric index for list, got '{part}'"
                ) from e
        elif isinstance(current, dict):
            if part not in current:
                raise ValueError(
                    f"Cannot resolve data_key '{data_key}': "
                    f"key '{part}' not found. Available keys: {list(current.keys())[:10]}"
                )
            current = current[part]
        else:
            raise ValueError(
                f"Cannot resolve data_key '{data_key}': "
                f"unexpected type {type(current).__name__} at '{part}'"
            )

    if not isinstance(current, list):
        raise ValueError(
            f"data_key '{data_key}' resolved to {type(current).__name__}, "
            f"expected a list of records"
        )
    return current


def fetch_and_detect_api_schema(
    api_endpoint: str,
    api_method: str = "GET",
    api_headers: dict | None = None,
    api_body: dict | None = None,
    api_auth_type: str | None = None,
    api_credentials: dict | None = None,
    data_key: str | None = None,
    pipeline_id: UUID | None = None,
    sample_rows: int = 1000,
) -> SchemaDetectionResult:
    """
    Fetch data from a REST API endpoint, detect schema from the response.

    Steps:
    1. Make the HTTP request with provided auth / headers
    2. Extract records using data_key (dot-notation for nested JSON)
    3. Convert to pandas DataFrame
    4. Reuse _analyze_field() for type detection on each column
    5. Return SchemaDetectionResult with virtual file info
    """
    # -- Build auth -----------------------------------------------------------
    auth = None
    headers = dict(api_headers or {})

    # Ensure a User-Agent is set (some APIs like Wikimedia reject requests without one)
    if "User-Agent" not in headers and "user-agent" not in headers:
        headers["User-Agent"] = "AutonomousPipeline/1.0"

    if api_auth_type == "basic" and api_credentials:
        auth = (api_credentials.get("username", ""), api_credentials.get("password", ""))
    elif api_auth_type == "bearer" and api_credentials:
        token = api_credentials.get("token", "")
        headers["Authorization"] = f"Bearer {token}"

    # -- Make the HTTP request ------------------------------------------------
    logger.info(f"Fetching API schema from {api_method} {api_endpoint}")
    try:
        response = requests.request(
            method=api_method.upper(),
            url=api_endpoint,
            headers=headers,
            json=api_body if api_body else None,
            timeout=30,
        )
        response.raise_for_status()
    except requests.RequestException as e:
        raise ValueError(f"API request failed: {e}") from e

    # -- Parse response -------------------------------------------------------
    try:
        data = response.json()
    except ValueError as e:
        raise ValueError(
            f"API did not return valid JSON. "
            f"Content-Type: {response.headers.get('Content-Type', 'unknown')}"
        ) from e

    # -- Extract records using data_key ---------------------------------------
    if data_key:
        records = _resolve_data_key(data, data_key)
    elif isinstance(data, list):
        records = data
    elif isinstance(data, dict):
        # Try common patterns: results, data, items, records
        for candidate in ("results", "data", "items", "records", "rows"):
            if candidate in data and isinstance(data[candidate], list):
                records = data[candidate]
                logger.info(f"Auto-detected data_key='{candidate}' ({len(records)} records)")
                break
        else:
            # Wrap the single object as a one-element list
            records = [data]
    else:
        raise ValueError(f"Unexpected API response type: {type(data).__name__}")

    if not records:
        raise ValueError("API returned 0 records. Check the endpoint and data_key.")

    # -- Convert to DataFrame -------------------------------------------------
    df = pd.DataFrame(records[:sample_rows])

    if df.empty:
        raise ValueError("Converted DataFrame is empty. API records may be malformed.")

    logger.info(f"API returned {len(records)} records, sampling {len(df)} rows with {len(df.columns)} columns")

    # -- Detect schema using existing field analysis --------------------------
    fields: list[FieldSchema] = []
    for col in df.columns:
        field = _analyze_field(col, df[col], len(df))
        fields.append(field)

    # -- Confidence -----------------------------------------------------------
    field_confidences = [f.confidence for f in fields]
    avg_confidence = sum(field_confidences) / len(field_confidences) if field_confidences else 0.0
    overall_confidence = round(avg_confidence, 3)

    # -- Build a virtual FileInfo for the API endpoint ------------------------
    virtual_file = FileInfo(
        path=api_endpoint,
        filename=api_endpoint.split("/")[-1] or "api_response",
        size_bytes=len(response.content),
        row_count=len(records),
        schema_match=True,
    )

    return SchemaDetectionResult(
        pipeline_id=pipeline_id,
        schema_version=1,
        fields=fields,
        total_files=1,
        compatible_files=[virtual_file],
        incompatible_files=[],
        sample_row_count=len(df),
        detection_confidence=overall_confidence,
        detected_at=datetime.now(timezone.utc),
    )
