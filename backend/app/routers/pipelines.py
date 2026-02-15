"""
Pipeline management API routes.
CRUD operations for pipelines.
"""

import logging
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from backend.app.database import get_db
from backend.app.models.models import Pipeline, DataSource, AuditLog
from backend.app.schemas.pipeline import (
    PipelineCreate, PipelineUpdate, PipelineResponse,
    DataSourceCreate, DataSourceResponse,
    SourceType,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/pipelines", tags=["Pipelines"])


# =============================================================================
# Pipeline CRUD
# =============================================================================

@router.post("/", response_model=PipelineResponse, status_code=201)
def create_pipeline(payload: PipelineCreate, db: Session = Depends(get_db)):
    """Create a new pipeline with source type selection."""
    pipeline = Pipeline(
        name=payload.name,
        description=payload.description,
        source_type=payload.source_type.value,
        status="draft",
    )
    db.add(pipeline)
    db.commit()
    db.refresh(pipeline)

    # Audit log
    db.add(AuditLog(
        entity_type="pipeline",
        entity_id=pipeline.id,
        action="created",
        new_value={"name": pipeline.name, "source_type": pipeline.source_type},
    ))
    db.commit()

    logger.info(f"Pipeline created: {pipeline.id} ({pipeline.name})")
    return pipeline


@router.get("/", response_model=list[PipelineResponse])
def list_pipelines(
    status: Optional[str] = Query(None),
    source_type: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """List all pipelines, optionally filtered by status or source type."""
    query = db.query(Pipeline)
    if status:
        query = query.filter(Pipeline.status == status)
    if source_type:
        query = query.filter(Pipeline.source_type == source_type)
    return query.order_by(Pipeline.created_at.desc()).all()


@router.get("/{pipeline_id}", response_model=PipelineResponse)
def get_pipeline(pipeline_id: UUID, db: Session = Depends(get_db)):
    """Get a single pipeline by ID."""
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return pipeline


@router.put("/{pipeline_id}", response_model=PipelineResponse)
def update_pipeline(pipeline_id: UUID, payload: PipelineUpdate, db: Session = Depends(get_db)):
    """Update pipeline name, description, or status."""
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    old_values = {"name": pipeline.name, "status": pipeline.status}

    if payload.name is not None:
        pipeline.name = payload.name
    if payload.description is not None:
        pipeline.description = payload.description
    if payload.source_type is not None:
        pipeline.source_type = payload.source_type.value
    if payload.status is not None:
        pipeline.status = payload.status.value

    db.commit()
    db.refresh(pipeline)

    db.add(AuditLog(
        entity_type="pipeline",
        entity_id=pipeline.id,
        action="updated",
        old_value=old_values,
        new_value={"name": pipeline.name, "status": pipeline.status},
    ))
    db.commit()

    return pipeline


@router.delete("/{pipeline_id}", status_code=204)
def delete_pipeline(pipeline_id: UUID, db: Session = Depends(get_db)):
    """Delete a pipeline and all associated data."""
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    db.add(AuditLog(
        entity_type="pipeline",
        entity_id=pipeline.id,
        action="deleted",
        old_value={"name": pipeline.name},
    ))
    db.delete(pipeline)
    db.commit()
    logger.info(f"Pipeline deleted: {pipeline_id}")


# =============================================================================
# Data Source Configuration
# =============================================================================

@router.post("/{pipeline_id}/source", response_model=DataSourceResponse, status_code=201)
def configure_data_source(
    pipeline_id: UUID,
    payload: DataSourceCreate,
    db: Session = Depends(get_db),
):
    """Configure the data source for a pipeline."""
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    # Remove existing data source if any
    existing = db.query(DataSource).filter(DataSource.pipeline_id == pipeline_id).first()
    if existing:
        db.delete(existing)

    # Build data source record based on source type
    ds = DataSource(
        pipeline_id=pipeline_id,
        source_type=payload.source_type.value,
    )

    if payload.source_type in (SourceType.csv, SourceType.json, SourceType.parquet):
        if not payload.local_config:
            raise HTTPException(400, "local_config required for file-based sources")
        ds.file_path = payload.local_config.file_path
        ds.file_format = payload.local_config.file_format.value
        ds.config = {
            "csv_delimiter": payload.local_config.csv_delimiter,
            "csv_header": payload.local_config.csv_header,
            "csv_encoding": payload.local_config.csv_encoding,
        }

    elif payload.source_type == SourceType.api:
        if not payload.api_config:
            raise HTTPException(400, "api_config required for API sources")
        ds.api_endpoint = payload.api_config.api_endpoint
        ds.api_method = payload.api_config.api_method
        ds.api_headers = payload.api_config.api_headers
        ds.api_body = payload.api_config.api_body
        ds.api_auth_type = payload.api_config.api_auth_type
        ds.api_credentials = payload.api_config.api_credentials
        ds.config = {
            "data_key": payload.api_config.data_key,
            "pagination": payload.api_config.pagination,
        }

    elif payload.source_type == SourceType.kafka:
        if not payload.kafka_config:
            raise HTTPException(400, "kafka_config required for Kafka sources")
        ds.kafka_bootstrap = payload.kafka_config.kafka_bootstrap
        ds.kafka_topic = payload.kafka_config.kafka_topic
        ds.kafka_group_id = payload.kafka_config.kafka_group_id
        ds.kafka_config = payload.kafka_config.kafka_config or {}
        ds.config = ds.kafka_config

    elif payload.source_type == SourceType.database:
        if not payload.database_config:
            raise HTTPException(400, "database_config required for DB sources")
        ds.db_connection = payload.database_config.db_connection
        ds.db_query = payload.database_config.db_query
        ds.db_table = payload.database_config.db_table

    db.add(ds)

    # Sync pipeline source_type and advance status for API/Kafka sources
    pipeline.source_type = payload.source_type.value
    if payload.source_type in (SourceType.api, SourceType.kafka):
        # API/Kafka don't have schema detection, mark as configured
        if pipeline.status == "draft":
            pipeline.status = "bronze_ready"

    db.commit()
    db.refresh(ds)

    logger.info(f"Data source configured for pipeline {pipeline_id}: {ds.source_type}")
    return ds
