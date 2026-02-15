"""
Pydantic schemas for schema detection, preview, and confirmation.
"""

from pydantic import BaseModel, Field
from typing import Optional, Any
from datetime import datetime
from uuid import UUID


# =============================================================================
# Schema Detection
# =============================================================================
class FieldSchema(BaseModel):
    """A single field in the detected schema."""
    name: str
    detected_type: str                         # string, integer, float, boolean, timestamp, date
    nullable: bool = True
    confidence: float = 1.0                    # 0.0 to 1.0
    sample_values: list[Any] = []              # Up to 5 sample values
    unique_count: int = 0
    null_count: int = 0
    total_count: int = 0
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None


class FileInfo(BaseModel):
    """Info about a single file in the scanned directory."""
    path: str
    filename: str
    size_bytes: int
    row_count: Optional[int] = None
    schema_match: bool = True
    mismatch_reason: Optional[str] = None


class SchemaDetectionResult(BaseModel):
    """Result from scanning a directory and detecting schema."""
    pipeline_id: UUID
    schema_id: Optional[UUID] = None
    schema_version: int
    fields: list[FieldSchema]
    # File scan results
    total_files: int
    compatible_files: list[FileInfo]
    incompatible_files: list[FileInfo]
    # Metadata
    sample_row_count: int
    detection_confidence: float
    detected_at: datetime


# =============================================================================
# Schema Confirmation (user edits + confirms)
# =============================================================================
class FieldOverride(BaseModel):
    """User's override for a single field."""
    name: str
    new_type: Optional[str] = None       # Override detected type
    new_name: Optional[str] = None       # Rename the field
    nullable: Optional[bool] = None      # Override nullable
    exclude: bool = False                # If True, drop this field (still stored in Bronze, but marked)


class SchemaConfirmRequest(BaseModel):
    """User confirms schema, optionally with overrides."""
    pipeline_id: UUID
    schema_id: UUID
    field_overrides: list[FieldOverride] = []
    # User can exclude specific incompatible files or force-include them
    exclude_files: list[str] = []         # File paths to exclude
    include_files: list[str] = []         # Force include despite mismatch


class SchemaConfirmResponse(BaseModel):
    """Response after schema confirmation + Bronze ingestion starts."""
    pipeline_id: UUID
    schema_id: UUID
    schema_version: int
    status: str
    bronze_path: str
    total_files_to_ingest: int
    message: str


# =============================================================================
# Schema Registry Response
# =============================================================================
class SchemaRegistryResponse(BaseModel):
    id: UUID
    pipeline_id: UUID
    version: int
    fields: list[dict]
    total_files: int
    compatible_files: list[dict]
    incompatible_files: list[dict]
    sample_row_count: int
    detection_confidence: float
    user_modified: bool
    status: str
    confirmed_at: Optional[datetime]
    created_at: datetime

    class Config:
        from_attributes = True
