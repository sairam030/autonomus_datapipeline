"""
Schema detection and confirmation API routes.

Flow:
  POST /api/schemas/detect     → Scan directory, detect schema, return preview
  POST /api/schemas/confirm    → User confirms schema, trigger Bronze ingestion
  GET  /api/schemas/{pipeline_id}/versions → List schema versions for a pipeline
  GET  /api/schemas/{schema_id} → Get specific schema details
"""

import logging
import re
from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session

from backend.app.database import get_db
from backend.app.models.models import (
    Pipeline, DataSource, SchemaRegistry, BronzeIngestion, AuditLog,
)
from backend.app.schemas.schema import (
    SchemaDetectionResult, SchemaConfirmRequest, SchemaConfirmResponse,
    SchemaRegistryResponse,
)
from backend.app.services.schema_detection import scan_and_detect_schema, fetch_and_detect_api_schema
from backend.app.services.ingestion.bronze_ingestion import ingest_to_bronze
from backend.app.services.code_saver import save_bronze_ingestion

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/schemas", tags=["Schema Detection"])


# =============================================================================
# Schema Detection
# =============================================================================

@router.post("/detect", response_model=SchemaDetectionResult)
def detect_schema(pipeline_id: UUID, db: Session = Depends(get_db)):
    """
    Scan the configured data source directory, detect schema, and return preview.
    User must have already configured the data source via POST /api/pipelines/{id}/source.
    """
    # Load pipeline and data source
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    if not pipeline:
        raise HTTPException(404, "Pipeline not found")

    data_source = db.query(DataSource).filter(DataSource.pipeline_id == pipeline_id).first()
    if not data_source:
        raise HTTPException(400, "No data source configured. Configure source first via POST /api/pipelines/{id}/source")

    if data_source.source_type not in ("csv", "json", "parquet", "api"):
        raise HTTPException(400, f"Schema detection for '{data_source.source_type}' not yet supported. Use csv, json, parquet, or api.")

    # Get current max schema version for this pipeline
    max_version = (
        db.query(SchemaRegistry.version)
        .filter(SchemaRegistry.pipeline_id == pipeline_id)
        .order_by(SchemaRegistry.version.desc())
        .first()
    )
    next_version = (max_version[0] + 1) if max_version else 1

    # Run schema detection — branch by source type
    try:
        if data_source.source_type == "api":
            # API source: fetch from endpoint and detect schema from response
            if not data_source.api_endpoint:
                raise HTTPException(400, "api_endpoint not configured in data source")

            config = data_source.config or {}
            result = fetch_and_detect_api_schema(
                api_endpoint=data_source.api_endpoint,
                api_method=data_source.api_method or "GET",
                api_headers=data_source.api_headers or {},
                api_body=data_source.api_body or {},
                api_auth_type=data_source.api_auth_type,
                api_credentials=data_source.api_credentials or {},
                data_key=config.get("data_key"),
                pipeline_id=pipeline_id,
            )
        else:
            # File source: scan directory
            if not data_source.file_path:
                raise HTTPException(400, "file_path not configured in data source")

            config = data_source.config or {}
            csv_delimiter = config.get("csv_delimiter", ",")
            csv_header = config.get("csv_header", True)
            csv_encoding = config.get("csv_encoding", "utf-8")

            result = scan_and_detect_schema(
                directory_path=data_source.file_path,
                file_format=data_source.file_format or data_source.source_type,
                pipeline_id=pipeline_id,
                csv_delimiter=csv_delimiter,
                csv_header=csv_header,
                csv_encoding=csv_encoding,
            )
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        logger.error(f"Schema detection failed: {e}", exc_info=True)
        raise HTTPException(500, f"Schema detection failed: {str(e)}")

    # Update version
    result.schema_version = next_version

    # Save to schema_registry (upsert — update if same version exists unconfirmed)
    existing_schema = (
        db.query(SchemaRegistry)
        .filter(
            SchemaRegistry.pipeline_id == pipeline_id,
            SchemaRegistry.version == next_version,
        )
        .first()
    )

    if existing_schema and existing_schema.status != "confirmed":
        # Update existing unconfirmed schema in-place
        existing_schema.fields = [f.model_dump() for f in result.fields]
        existing_schema.total_files = result.total_files
        existing_schema.compatible_files = [f.model_dump() for f in result.compatible_files]
        existing_schema.incompatible_files = [f.model_dump() for f in result.incompatible_files]
        existing_schema.sample_row_count = result.sample_row_count
        existing_schema.detection_confidence = result.detection_confidence
        existing_schema.status = "detected"
        schema_record = existing_schema
    elif existing_schema and existing_schema.status == "confirmed":
        # Bump to next version if current is already confirmed
        next_version += 1
        schema_record = SchemaRegistry(
            pipeline_id=pipeline_id,
            version=next_version,
            fields=[f.model_dump() for f in result.fields],
            total_files=result.total_files,
            compatible_files=[f.model_dump() for f in result.compatible_files],
            incompatible_files=[f.model_dump() for f in result.incompatible_files],
            sample_row_count=result.sample_row_count,
            detection_confidence=result.detection_confidence,
            status="detected",
        )
        db.add(schema_record)
    else:
        schema_record = SchemaRegistry(
            pipeline_id=pipeline_id,
            version=next_version,
            fields=[f.model_dump() for f in result.fields],
            total_files=result.total_files,
            compatible_files=[f.model_dump() for f in result.compatible_files],
            incompatible_files=[f.model_dump() for f in result.incompatible_files],
            sample_row_count=result.sample_row_count,
            detection_confidence=result.detection_confidence,
            status="detected",
        )
        db.add(schema_record)

    # Update pipeline status
    pipeline.status = "schema_detected"
    db.commit()
    db.refresh(schema_record)

    # Put the schema_id into the result for the frontend
    result.pipeline_id = pipeline_id
    result.schema_version = next_version
    result.schema_id = schema_record.id

    logger.info(
        f"Schema detected for pipeline {pipeline_id}: "
        f"{len(result.fields)} fields, {result.total_files} files, "
        f"confidence={result.detection_confidence}"
    )

    return result


# =============================================================================
# Schema Confirmation + Bronze Ingestion
# =============================================================================

def _run_bronze_ingestion(
    pipeline_id: UUID,
    schema_version: int,
    file_format: str,
    compatible_files: list[str],
    fields: list[dict],
    csv_config: dict,
    ingestion_id: UUID,
    db_url: str,
    pipeline_name: str = "",
):
    """Background task: run Spark Bronze ingestion and update DB."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SyncSession

    engine = create_engine(db_url)
    session = SyncSession(engine)

    try:
        result = ingest_to_bronze(
            pipeline_id=pipeline_id,
            schema_version=schema_version,
            file_format=file_format,
            compatible_files=compatible_files,
            fields=fields,
            csv_delimiter=csv_config.get("csv_delimiter", ","),
            csv_header=csv_config.get("csv_header", True),
            csv_encoding=csv_config.get("csv_encoding", "utf-8"),
            pipeline_name=pipeline_name,
        )

        # Update ingestion record
        ingestion = session.query(BronzeIngestion).filter(BronzeIngestion.id == ingestion_id).first()
        if ingestion:
            ingestion.status = result["status"]
            ingestion.total_records = result["total_records"]
            ingestion.files_ingested = result["files_ingested"]
            ingestion.files_skipped = result["files_skipped"]
            ingestion.duration_seconds = result["duration_seconds"]
            ingestion.error_message = result.get("error_message")
            ingestion.completed_at = datetime.now(timezone.utc)

        # Update pipeline status
        pipeline = session.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
        if pipeline:
            pipeline.status = "bronze_ready" if result["status"] == "success" else "error"

        session.commit()
        logger.info(f"Bronze ingestion completed for pipeline {pipeline_id}: {result['status']}")

    except Exception as e:
        logger.error(f"Bronze ingestion background task failed: {e}", exc_info=True)
        ingestion = session.query(BronzeIngestion).filter(BronzeIngestion.id == ingestion_id).first()
        if ingestion:
            ingestion.status = "failed"
            ingestion.error_message = str(e)
            ingestion.completed_at = datetime.now(timezone.utc)

        pipeline = session.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
        if pipeline:
            pipeline.status = "error"

        session.commit()

    finally:
        session.close()
        engine.dispose()


@router.post("/confirm", response_model=SchemaConfirmResponse)
def confirm_schema(
    payload: SchemaConfirmRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    User confirms the detected schema (with optional overrides).
    This triggers Bronze ingestion as a background task.
    """
    from backend.app.config import get_settings
    settings = get_settings()

    # Load schema record
    schema_record = db.query(SchemaRegistry).filter(
        SchemaRegistry.id == payload.schema_id,
        SchemaRegistry.pipeline_id == payload.pipeline_id,
    ).first()

    if not schema_record:
        raise HTTPException(404, "Schema record not found")

    if schema_record.status == "confirmed":
        raise HTTPException(400, "Schema already confirmed")

    # Load data source
    data_source = db.query(DataSource).filter(
        DataSource.pipeline_id == payload.pipeline_id
    ).first()
    if not data_source:
        raise HTTPException(400, "No data source configured")

    # Apply user overrides to fields
    fields = schema_record.fields  # list of dicts
    if payload.field_overrides:
        override_map = {o.name: o for o in payload.field_overrides}
        for field in fields:
            if field["name"] in override_map:
                override = override_map[field["name"]]
                if override.new_type:
                    field["new_type"] = override.new_type
                if override.new_name:
                    field["new_name"] = override.new_name
                if override.nullable is not None:
                    field["nullable"] = override.nullable
                if override.exclude:
                    field["excluded"] = True

        schema_record.user_modified = True
        schema_record.user_overrides = {
            o.name: o.model_dump() for o in payload.field_overrides
        }

    # Update schema status
    schema_record.status = "confirmed"
    schema_record.confirmed_at = datetime.now(timezone.utc)
    schema_record.fields = fields

    # Determine files to ingest
    compatible_paths = [
        f["path"] for f in schema_record.compatible_files
        if f["path"] not in payload.exclude_files
    ]

    # Add force-included files
    for path in payload.include_files:
        if path not in compatible_paths:
            compatible_paths.append(path)

    # Update pipeline status
    pipeline = db.query(Pipeline).filter(Pipeline.id == payload.pipeline_id).first()
    pipeline.status = "schema_confirmed"

    # Create Bronze ingestion record
    # Use pipeline name slug for readable MinIO paths
    pipeline_slug = re.sub(r"[^a-z0-9]+", "_", pipeline.name.lower()).strip("_")
    bronze_path = f"s3a://{settings.bronze_bucket}/{pipeline_slug}/v{schema_record.version}/data"
    ingestion = BronzeIngestion(
        pipeline_id=payload.pipeline_id,
        schema_version=schema_record.version,
        bronze_path=bronze_path,
        status="running" if data_source.source_type not in ("api", "kafka") else "success",
    )
    db.add(ingestion)
    db.commit()
    db.refresh(ingestion)

    if data_source.source_type in ("api", "kafka"):
        # For API / Kafka sources, Bronze ingestion is handled by the
        # generated Airflow DAG, not by a local Spark job.  Just mark
        # the pipeline as bronze_ready so users can proceed to DAG creation.
        pipeline.status = "bronze_ready"
        ingestion.total_records = 0
        ingestion.files_ingested = 0
        ingestion.files_skipped = 0
        ingestion.duration_seconds = 0.0
        ingestion.completed_at = datetime.now(timezone.utc)
        db.commit()

        logger.info(
            f"Schema confirmed for {data_source.source_type} pipeline {payload.pipeline_id}. "
            f"Bronze ingestion will be handled by the Airflow DAG."
        )

        return SchemaConfirmResponse(
            pipeline_id=payload.pipeline_id,
            schema_id=schema_record.id,
            schema_version=schema_record.version,
            status="confirmed",
            bronze_path=bronze_path,
            total_files_to_ingest=0,
            message=(
                f"Schema confirmed. For {data_source.source_type} sources, "
                f"Bronze ingestion will be handled by the generated Airflow DAG. "
                f"Proceed to create a DAG."
            ),
        )

    # Kick off Bronze ingestion in background (file sources only)
    csv_config = data_source.config or {}

    # Save bronze ingestion config to local filesystem
    try:
        save_bronze_ingestion(
            project_name=pipeline.name,
            pipeline_id=str(payload.pipeline_id),
            schema_version=schema_record.version,
            file_format=data_source.file_format or data_source.source_type,
            compatible_files=compatible_paths,
            fields=fields,
            bronze_path=bronze_path,
            csv_config=csv_config,
        )
    except Exception as save_err:
        logger.warning("Could not save bronze ingestion config to disk: %s", save_err)

    background_tasks.add_task(
        _run_bronze_ingestion,
        pipeline_id=payload.pipeline_id,
        schema_version=schema_record.version,
        file_format=data_source.file_format or data_source.source_type,
        compatible_files=compatible_paths,
        fields=fields,
        csv_config=csv_config,
        ingestion_id=ingestion.id,
        db_url=settings.database_url,
        pipeline_name=pipeline.name,
    )

    logger.info(
        f"Schema confirmed for pipeline {payload.pipeline_id}. "
        f"Bronze ingestion started: {len(compatible_paths)} files"
    )

    return SchemaConfirmResponse(
        pipeline_id=payload.pipeline_id,
        schema_id=schema_record.id,
        schema_version=schema_record.version,
        status="ingestion_started",
        bronze_path=bronze_path,
        total_files_to_ingest=len(compatible_paths),
        message=f"Schema confirmed. Bronze ingestion started for {len(compatible_paths)} file(s).",
    )


# =============================================================================
# Schema Queries
# =============================================================================

@router.get("/{pipeline_id}/versions", response_model=list[SchemaRegistryResponse])
def list_schema_versions(pipeline_id: UUID, db: Session = Depends(get_db)):
    """List all schema versions for a pipeline."""
    schemas = (
        db.query(SchemaRegistry)
        .filter(SchemaRegistry.pipeline_id == pipeline_id)
        .order_by(SchemaRegistry.version.desc())
        .all()
    )
    return schemas


@router.get("/detail/{schema_id}", response_model=SchemaRegistryResponse)
def get_schema(schema_id: UUID, db: Session = Depends(get_db)):
    """Get a specific schema record."""
    schema = db.query(SchemaRegistry).filter(SchemaRegistry.id == schema_id).first()
    if not schema:
        raise HTTPException(404, "Schema not found")
    return schema
