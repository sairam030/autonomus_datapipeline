"""
Pydantic schemas for Bronze ingestion tracking.
"""

from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from uuid import UUID


class BronzeIngestionResponse(BaseModel):
    id: UUID
    pipeline_id: UUID
    schema_version: int
    files_ingested: list[dict]
    files_skipped: list[dict]
    bronze_path: str
    total_records: int
    total_size_bytes: int
    duration_seconds: float
    status: str
    error_message: Optional[str]
    started_at: datetime
    completed_at: Optional[datetime]

    class Config:
        from_attributes = True


class BronzeIngestionStatus(BaseModel):
    """Lightweight status check response."""
    ingestion_id: UUID
    pipeline_id: UUID
    status: str
    total_records: int
    files_processed: int
    files_skipped: int
    duration_seconds: float
    error_message: Optional[str]
