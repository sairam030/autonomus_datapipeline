"""
Pydantic schemas for request/response validation — Pipeline & DataSource.
"""

from pydantic import BaseModel, Field
from typing import Optional, Any
from datetime import datetime
from uuid import UUID
from enum import Enum


# =============================================================================
# Enums
# =============================================================================
class SourceType(str, Enum):
    csv = "csv"
    json = "json"
    parquet = "parquet"
    api = "api"
    kafka = "kafka"
    database = "database"


class PipelineStatus(str, Enum):
    draft = "draft"
    schema_detected = "schema_detected"
    schema_confirmed = "schema_confirmed"
    bronze_ready = "bronze_ready"
    silver_configured = "silver_configured"
    gold_configured = "gold_configured"
    gold_ready = "gold_ready"
    active = "active"
    paused = "paused"
    error = "error"


# =============================================================================
# Pipeline Schemas
# =============================================================================
class PipelineCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    source_type: SourceType


class PipelineUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    source_type: Optional[SourceType] = None
    status: Optional[PipelineStatus] = None


class PipelineResponse(BaseModel):
    id: UUID
    name: str
    description: Optional[str]
    source_type: SourceType
    status: PipelineStatus
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# =============================================================================
# DataSource Schemas
# =============================================================================
class LocalFileSourceConfig(BaseModel):
    """Config for CSV, JSON, Parquet local file sources."""
    file_path: str = Field(..., description="Absolute directory path containing data files")
    file_format: SourceType = Field(..., description="File format: csv, json, parquet")
    # CSV-specific options
    csv_delimiter: Optional[str] = ","
    csv_header: Optional[bool] = True
    csv_encoding: Optional[str] = "utf-8"


class ApiSourceConfig(BaseModel):
    """Config for API data sources."""
    api_endpoint: str
    api_method: str = "GET"
    api_headers: dict = {}
    api_body: dict = {}
    api_auth_type: Optional[str] = None
    api_credentials: dict = {}
    data_key: Optional[str] = None
    pagination: dict = {}


class KafkaSourceConfig(BaseModel):
    """Config for Kafka stream sources."""
    kafka_bootstrap: str
    kafka_topic: str
    kafka_group_id: Optional[str] = None
    kafka_config: dict = {}


class DatabaseSourceConfig(BaseModel):
    """Config for database sources."""
    db_connection: str
    db_query: Optional[str] = None
    db_table: Optional[str] = None


class DataSourceCreate(BaseModel):
    source_type: SourceType
    local_config: Optional[LocalFileSourceConfig] = None
    api_config: Optional[ApiSourceConfig] = None
    kafka_config: Optional[KafkaSourceConfig] = None
    database_config: Optional[DatabaseSourceConfig] = None


class DataSourceResponse(BaseModel):
    id: UUID
    pipeline_id: UUID
    source_type: str
    file_path: Optional[str]
    file_format: Optional[str]
    api_endpoint: Optional[str]
    kafka_topic: Optional[str]
    db_table: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True
