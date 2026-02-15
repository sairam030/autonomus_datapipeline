"""
DAG management router.

Endpoints for generating, listing, and deleting Airflow DAGs
via the template-based DAG generator — no LLM needed.
"""

import logging
from datetime import datetime
from typing import Optional, List
from uuid import UUID

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.app.database import get_db
from backend.app.models.models import Pipeline, DataSource
from backend.app.services.dag_generator import DAGGenerator

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/dags", tags=["DAGs"])


# ============================================================================
# Request / Response schemas
# ============================================================================

class DAGScheduleConfig(BaseModel):
    """Shared schedule/retry configuration."""
    schedule: Optional[str] = Field(
        None,
        description="Cron expression, e.g. '0 6 * * *' or '@daily'. None = manual only.",
        examples=["@daily", "0 */6 * * *", None],
    )
    start_date: Optional[str] = Field(
        "2024-01-01",
        description="DAG start date in YYYY-MM-DD format",
    )
    retries: int = Field(1, ge=0, le=10)
    retry_delay_min: int = Field(5, ge=1, le=60)
    owner: str = Field("autonomous-pipeline", max_length=100)

    # Source overrides (optional — uses project's configured source by default)
    source_type: Optional[str] = Field(
        None, description="Override source type: csv, json, parquet, api, kafka"
    )
    source_config: Optional[dict] = Field(
        None, description="Override source configuration"
    )


class TaskDAGRequest(BaseModel):
    """Request to create a single task DAG."""
    task_type: str = Field(
        ..., description="Task type: bronze, silver, gold",
        examples=["bronze", "silver", "gold"],
    )
    task_label: Optional[str] = Field(
        None,
        description="Custom label for this task (e.g. 'csv_ingest', 'api_fetch'). "
                    "Allows multiple DAGs of the same type.",
    )
    schedule: Optional[str] = None
    start_date: Optional[str] = "2024-01-01"
    retries: int = Field(1, ge=0, le=10)
    retry_delay_min: int = Field(5, ge=1, le=60)
    owner: str = Field("autonomous-pipeline", max_length=100)
    source_type: Optional[str] = None
    source_config: Optional[dict] = None


class MasterDAGRequest(BaseModel):
    """Request to create a master DAG that chains task DAGs in order."""
    dag_ids: List[str] = Field(
        ...,
        description="Ordered list of task DAG IDs. Master DAG will chain them in this order.",
        min_length=1,
    )
    schedule: Optional[str] = None
    start_date: Optional[str] = "2024-01-01"
    retries: int = Field(1, ge=0, le=10)
    retry_delay_min: int = Field(5, ge=1, le=60)
    owner: str = Field("autonomous-pipeline", max_length=100)


class DAGInfo(BaseModel):
    dag_id: str
    dag_type: str
    source_type: Optional[str] = None
    filename: str
    filepath: str
    schedule: Optional[str] = None
    child_dags: Optional[List[str]] = None


class GenerateDAGResponse(BaseModel):
    project_id: str
    project_name: str
    dags: List[DAGInfo]
    master: Optional[DAGInfo] = None
    message: str


class DAGFileInfo(BaseModel):
    filename: str
    filepath: str
    size_bytes: int
    modified_at: str


# ============================================================================
# Endpoints
# ============================================================================

@router.post("/{project_id}/task-dag", response_model=DAGInfo)
def create_task_dag(
    project_id: UUID,
    req: TaskDAGRequest,
    db: Session = Depends(get_db),
):
    """Create a single task DAG (bronze, silver, or gold)."""
    pipeline = db.query(Pipeline).filter(Pipeline.id == project_id).first()
    if not pipeline:
        raise HTTPException(404, "Project not found")

    # Validate task type
    if req.task_type not in ("bronze", "silver", "gold"):
        raise HTTPException(400, f"Invalid task_type: {req.task_type}. Must be bronze, silver, or gold.")

    # Load source config
    data_source = db.query(DataSource).filter(DataSource.pipeline_id == project_id).first()
    source_type = req.source_type or pipeline.source_type
    if req.source_config:
        source_config = req.source_config
    elif data_source:
        source_config = _build_source_config(data_source)
    else:
        source_config = {}

    # Parse start date
    try:
        start_date = datetime.strptime(req.start_date, "%Y-%m-%d") if req.start_date else None
    except ValueError:
        raise HTTPException(400, f"Invalid start_date format: {req.start_date}. Use YYYY-MM-DD.")

    generator = DAGGenerator()

    try:
        if req.task_type == "bronze":
            result = generator.generate_bronze_dag(
                project_name=pipeline.name,
                project_id=str(project_id),
                source_type=source_type,
                source_config=source_config,
                schedule=req.schedule,
                start_date=start_date,
                retries=req.retries,
                retry_delay_min=req.retry_delay_min,
                owner=req.owner,
                task_label=req.task_label,
            )
        elif req.task_type == "silver":
            # Silver DAG generation — placeholder for now
            result = generator.generate_bronze_dag(
                project_name=pipeline.name,
                project_id=str(project_id),
                source_type=source_type,
                source_config=source_config,
                schedule=req.schedule,
                start_date=start_date,
                retries=req.retries,
                retry_delay_min=req.retry_delay_min,
                owner=req.owner,
                task_label=req.task_label or "silver",
            )
            result["dag_type"] = "silver"
        elif req.task_type == "gold":
            # Gold DAG generation — placeholder for now
            result = generator.generate_bronze_dag(
                project_name=pipeline.name,
                project_id=str(project_id),
                source_type=source_type,
                source_config=source_config,
                schedule=req.schedule,
                start_date=start_date,
                retries=req.retries,
                retry_delay_min=req.retry_delay_min,
                owner=req.owner,
                task_label=req.task_label or "gold",
            )
            result["dag_type"] = "gold"
    except Exception as e:
        logger.error("Task DAG generation failed: %s", e, exc_info=True)
        raise HTTPException(500, f"Task DAG generation failed: {str(e)}")

    logger.info("Generated %s task DAG: %s for project %s", req.task_type, result["dag_id"], project_id)
    return DAGInfo(**result)


@router.post("/{project_id}/master-dag", response_model=DAGInfo)
def create_master_dag(
    project_id: UUID,
    req: MasterDAGRequest,
    db: Session = Depends(get_db),
):
    """Create a master DAG that chains task DAGs in the specified order."""
    pipeline = db.query(Pipeline).filter(Pipeline.id == project_id).first()
    if not pipeline:
        raise HTTPException(404, "Project not found")

    if not req.dag_ids or len(req.dag_ids) == 0:
        raise HTTPException(400, "dag_ids must have at least one task DAG ID")

    # Parse start date
    try:
        start_date = datetime.strptime(req.start_date, "%Y-%m-%d") if req.start_date else None
    except ValueError:
        raise HTTPException(400, f"Invalid start_date format: {req.start_date}. Use YYYY-MM-DD.")

    generator = DAGGenerator()

    try:
        result = generator.generate_master_dag(
            project_name=pipeline.name,
            project_id=str(project_id),
            task_dag_ids=req.dag_ids,
            schedule=req.schedule,
            start_date=start_date,
            retries=req.retries,
            retry_delay_min=req.retry_delay_min,
            owner=req.owner,
        )
    except Exception as e:
        logger.error("Master DAG generation failed: %s", e, exc_info=True)
        raise HTTPException(500, f"Master DAG generation failed: {str(e)}")

    # Update pipeline status
    pipeline.status = "active"
    pipeline.metadata_ = pipeline.metadata_ or {}
    pipeline.metadata_["dag_schedule"] = req.schedule
    pipeline.metadata_["dag_generated_at"] = datetime.utcnow().isoformat()
    pipeline.metadata_["master_dag_id"] = result["dag_id"]
    db.commit()

    logger.info("Generated Master DAG: %s for project %s", result["dag_id"], project_id)
    return DAGInfo(**result)


@router.post("/{project_id}/generate", response_model=GenerateDAGResponse)
def generate_dags(
    project_id: UUID,
    config: DAGScheduleConfig,
    db: Session = Depends(get_db),
):
    """
    Generate Airflow DAG files for a project (legacy endpoint).
    Produces individual task DAGs + a master DAG that chains them.
    """
    # Load project
    pipeline = db.query(Pipeline).filter(Pipeline.id == project_id).first()
    if not pipeline:
        raise HTTPException(404, "Project not found")

    # Load source config
    data_source = db.query(DataSource).filter(DataSource.pipeline_id == project_id).first()

    # Determine source type and config
    source_type = config.source_type or pipeline.source_type
    if config.source_config:
        source_config = config.source_config
    elif data_source:
        source_config = _build_source_config(data_source)
    else:
        source_config = {}

    # Parse start date
    try:
        start_date = datetime.strptime(config.start_date, "%Y-%m-%d") if config.start_date else None
    except ValueError:
        raise HTTPException(400, f"Invalid start_date format: {config.start_date}. Use YYYY-MM-DD.")

    # Generate
    generator = DAGGenerator()
    try:
        result = generator.generate_full_pipeline(
            project_name=pipeline.name,
            project_id=str(project_id),
            source_type=source_type,
            source_config=source_config,
            schedule=config.schedule,
            start_date=start_date,
            retries=config.retries,
            retry_delay_min=config.retry_delay_min,
            owner=config.owner,
        )
    except Exception as e:
        logger.error("DAG generation failed: %s", e, exc_info=True)
        raise HTTPException(500, f"DAG generation failed: {str(e)}")

    # Update pipeline status
    pipeline.status = "active"
    pipeline.metadata_ = pipeline.metadata_ or {}
    pipeline.metadata_["dag_schedule"] = config.schedule
    pipeline.metadata_["dag_generated_at"] = datetime.utcnow().isoformat()
    pipeline.metadata_["master_dag_id"] = result["master"]["dag_id"] if result["master"] else None
    db.commit()

    logger.info(
        "Generated %d DAGs + master for project %s (%s)",
        len(result["dags"]), pipeline.name, project_id,
    )

    return GenerateDAGResponse(
        project_id=str(project_id),
        project_name=pipeline.name,
        dags=[DAGInfo(**d) for d in result["dags"]],
        master=DAGInfo(**result["master"]) if result["master"] else None,
        message=f"Generated {len(result['dags'])} task DAG(s) + master DAG",
    )


@router.get("/{project_id}", response_model=List[DAGFileInfo])
def list_project_dags(project_id: UUID, db: Session = Depends(get_db)):
    """List all generated DAG files for a project."""
    pipeline = db.query(Pipeline).filter(Pipeline.id == project_id).first()
    if not pipeline:
        raise HTTPException(404, "Project not found")

    generator = DAGGenerator()
    files = generator.list_generated_dags(str(project_id))
    return [DAGFileInfo(**f) for f in files]


@router.delete("/{project_id}")
def delete_project_dags(project_id: UUID, db: Session = Depends(get_db)):
    """Delete all DAG files for a project."""
    pipeline = db.query(Pipeline).filter(Pipeline.id == project_id).first()
    if not pipeline:
        raise HTTPException(404, "Project not found")

    generator = DAGGenerator()
    count = generator.delete_project_dags(str(project_id))

    # Update pipeline status back
    if pipeline.status == "active":
        pipeline.status = "bronze_ready"
        pipeline.metadata_ = pipeline.metadata_ or {}
        pipeline.metadata_.pop("dag_schedule", None)
        pipeline.metadata_.pop("dag_generated_at", None)
        pipeline.metadata_.pop("master_dag_id", None)
        db.commit()

    return {"deleted": count, "message": f"Deleted {count} DAG file(s)"}


@router.delete("/{project_id}/{filename}")
def delete_single_dag(project_id: UUID, filename: str, db: Session = Depends(get_db)):
    """Delete a specific DAG file."""
    pipeline = db.query(Pipeline).filter(Pipeline.id == project_id).first()
    if not pipeline:
        raise HTTPException(404, "Project not found")

    generator = DAGGenerator()
    if not generator.delete_dag(filename):
        raise HTTPException(404, f"DAG file not found: {filename}")

    return {"deleted": filename}


# ============================================================================
# Helpers
# ============================================================================

def _build_source_config(ds: DataSource) -> dict:
    """Build a source_config dict from a DataSource ORM object."""
    cfg = dict(ds.config or {})

    if ds.source_type in ("csv", "json", "parquet"):
        cfg.setdefault("file_path", ds.file_path or "")
        cfg.setdefault("file_format", ds.file_format or ds.source_type)
        if ds.source_type == "csv":
            local = cfg.get("local_config", cfg)
            cfg.setdefault("csv_delimiter", local.get("csv_delimiter", ","))
            cfg.setdefault("csv_header", local.get("csv_header", True))

    elif ds.source_type == "api":
        cfg.setdefault("endpoint", ds.api_endpoint or "")
        cfg.setdefault("method", ds.api_method or "GET")
        cfg.setdefault("headers", ds.api_headers or {})
        cfg.setdefault("body", ds.api_body or {})
        cfg.setdefault("auth_type", ds.api_auth_type or "none")
        cfg.setdefault("credentials", ds.api_credentials or {})

    elif ds.source_type == "kafka":
        cfg.setdefault("bootstrap_servers", ds.kafka_bootstrap or "")
        cfg.setdefault("topic", ds.kafka_topic or "")
        cfg.setdefault("group_id", ds.kafka_group_id or "")
        extra = ds.kafka_config or {}
        cfg.setdefault("max_messages", extra.get("max_messages", 10000))
        cfg.setdefault("consumer_timeout_ms", extra.get("consumer_timeout_ms", 30000))
        cfg.setdefault("auto_offset_reset", extra.get("auto_offset_reset", "earliest"))

    return cfg
