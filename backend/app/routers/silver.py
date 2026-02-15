"""
Silver Transformation Router — AI-driven enrichment with versioning.

Flow:
  1. Create transformations (each is a named step, e.g., "Filter Ground Flights")
  2. Chat with AI → generates PySpark code
  3. Dry-run on sample data to validate
  4. Confirm with a name → becomes version 1
  5. User can re-edit → new version (v2, v3, …)
  6. Create multiple transformations in sequence
  7. "Upload to Silver" applies all confirmed transforms in order → writes to silver bucket
"""

import logging
import time
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.app.database import get_db
from backend.app.models.models import (
    Pipeline, SchemaRegistry, SilverTransformation, ConversationMessage,
    SilverExecution, BronzeIngestion, GoldTransformation,
)
from backend.app.services.ai_service import generate_transformation, validate_transform_code
from backend.app.services.code_saver import (
    save_silver_ai_generated, save_silver_confirmed,
    save_silver_dry_run, save_code_edit,
)
from backend.app.services.sandbox import build_sample_rows_from_schema, execute_dry_run
from backend.app.services.silver_service import execute_silver_upload
from backend.app.services.spark_utils import (
    get_silver_schema_and_samples, preview_data_from_path,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/silver", tags=["Silver Transformations"])


# ============================================================================
# Request / Response schemas
# ============================================================================

class CreateTransformationRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1)


class UpdateCodeRequest(BaseModel):
    code: str


class ConfirmRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=255, description="Name for this version")
    code: str = Field(..., description="Final PySpark code")


class MessageResponse(BaseModel):
    id: str
    role: str
    content: str
    code_block: Optional[str] = None
    dry_run_result: Optional[dict] = None
    message_order: int
    created_at: str

    class Config:
        from_attributes = True


class TransformationResponse(BaseModel):
    id: str
    pipeline_id: str
    name: str
    description: Optional[str]
    status: str
    generated_code: Optional[str]
    confirmed_code: Optional[str]
    input_schema: list
    sample_input: list
    sample_output: list
    conversation_count: int
    task_order: int
    version: int
    is_active: bool
    created_at: str
    updated_at: str

    class Config:
        from_attributes = True


class ChatResponse(BaseModel):
    type: str           # "clarification", "code", "error"
    content: str
    code: Optional[str] = None
    message_id: str
    transformation_status: str


class DryRunResponse(BaseModel):
    success: bool
    output_rows: list = []
    output_schema: list = []
    row_count: int = 0
    error: Optional[str] = None
    validation_message: str = ""


class UploadToSilverRequest(BaseModel):
    transformation_ids: Optional[List[str]] = None  # if None, use all confirmed


class UploadToSilverResponse(BaseModel):
    success: bool
    execution_id: str
    input_records: int
    output_records: int
    output_path: str
    duration_seconds: float
    transformations_applied: int
    error: Optional[str] = None
    transform_results: list = []


# ============================================================================
# Helpers
# ============================================================================

def _get_transformation(db: Session, project_id: UUID, transform_id: UUID) -> SilverTransformation:
    t = (
        db.query(SilverTransformation)
        .filter(
            SilverTransformation.id == transform_id,
            SilverTransformation.pipeline_id == project_id,
        )
        .first()
    )
    if not t:
        raise HTTPException(404, "Transformation not found")
    return t


def _to_response(t: SilverTransformation) -> TransformationResponse:
    return TransformationResponse(
        id=str(t.id),
        pipeline_id=str(t.pipeline_id),
        name=t.name,
        description=t.description,
        status=t.status,
        generated_code=t.generated_code,
        confirmed_code=t.confirmed_code,
        input_schema=t.input_schema or [],
        sample_input=t.sample_input or [],
        sample_output=t.sample_output or [],
        conversation_count=t.conversation_count or 0,
        task_order=t.task_order or 1,
        version=t.version or 1,
        is_active=t.is_active if t.is_active is not None else True,
        created_at=t.created_at.isoformat() if t.created_at else "",
        updated_at=t.updated_at.isoformat() if t.updated_at else "",
    )


# ============================================================================
# CRUD Endpoints
# ============================================================================

@router.post("/{project_id}/transformations", response_model=TransformationResponse)
def create_transformation(
    project_id: UUID,
    req: CreateTransformationRequest,
    db: Session = Depends(get_db),
):
    """Create a new Silver transformation for a project."""
    pipeline = db.query(Pipeline).filter(Pipeline.id == project_id).first()
    if not pipeline:
        raise HTTPException(404, "Project not found")

    schema_reg = (
        db.query(SchemaRegistry)
        .filter(SchemaRegistry.pipeline_id == project_id)
        .order_by(SchemaRegistry.version.desc())
        .first()
    )
    input_schema = schema_reg.fields if schema_reg else []
    sample_input = build_sample_rows_from_schema(input_schema, num_rows=10)

    existing_count = (
        db.query(SilverTransformation)
        .filter(SilverTransformation.pipeline_id == project_id)
        .count()
    )

    transformation = SilverTransformation(
        pipeline_id=project_id,
        name=req.name,
        description=req.description,
        input_schema=input_schema,
        sample_input=sample_input,
        status="draft",
        task_order=existing_count + 1,
        version=1,
        is_active=True,
    )
    db.add(transformation)
    db.commit()
    db.refresh(transformation)

    logger.info("Created Silver transformation '%s' for project %s", req.name, project_id)
    return _to_response(transformation)


@router.get("/{project_id}/transformations", response_model=List[TransformationResponse])
def list_transformations(
    project_id: UUID,
    active_only: bool = False,
    db: Session = Depends(get_db),
):
    """List Silver transformations. Use active_only=true to get only the latest active versions."""
    pipeline = db.query(Pipeline).filter(Pipeline.id == project_id).first()
    if not pipeline:
        raise HTTPException(404, "Project not found")

    q = db.query(SilverTransformation).filter(SilverTransformation.pipeline_id == project_id)
    if active_only:
        q = q.filter(SilverTransformation.is_active == True)
    transformations = q.order_by(SilverTransformation.task_order, SilverTransformation.version.desc()).all()
    return [_to_response(t) for t in transformations]


@router.get("/{project_id}/transformations/{transform_id}", response_model=TransformationResponse)
def get_transformation(
    project_id: UUID,
    transform_id: UUID,
    db: Session = Depends(get_db),
):
    t = _get_transformation(db, project_id, transform_id)
    return _to_response(t)


@router.delete("/{project_id}/transformations/{transform_id}")
def delete_transformation(
    project_id: UUID,
    transform_id: UUID,
    db: Session = Depends(get_db),
):
    t = _get_transformation(db, project_id, transform_id)
    db.delete(t)
    db.commit()
    return {"deleted": str(transform_id)}


# ============================================================================
# Chat / Conversation
# ============================================================================

@router.get("/{project_id}/transformations/{transform_id}/messages", response_model=List[MessageResponse])
def get_messages(
    project_id: UUID,
    transform_id: UUID,
    db: Session = Depends(get_db),
):
    _get_transformation(db, project_id, transform_id)
    messages = (
        db.query(ConversationMessage)
        .filter(ConversationMessage.transformation_id == transform_id)
        .order_by(ConversationMessage.message_order)
        .all()
    )
    return [
        MessageResponse(
            id=str(m.id),
            role=m.role,
            content=m.content,
            code_block=m.code_block,
            dry_run_result=m.dry_run_result,
            message_order=m.message_order,
            created_at=m.created_at.isoformat() if m.created_at else "",
        )
        for m in messages
    ]


@router.post("/{project_id}/transformations/{transform_id}/chat", response_model=ChatResponse)
def send_chat_message(
    project_id: UUID,
    transform_id: UUID,
    req: ChatRequest,
    db: Session = Depends(get_db),
):
    t = _get_transformation(db, project_id, transform_id)

    existing_messages = (
        db.query(ConversationMessage)
        .filter(ConversationMessage.transformation_id == transform_id)
        .order_by(ConversationMessage.message_order)
        .all()
    )
    conversation_history = [
        {"role": m.role, "content": m.content}
        for m in existing_messages
    ]
    next_order = len(existing_messages) + 1

    user_msg = ConversationMessage(
        transformation_id=transform_id,
        role="user",
        content=req.message,
        message_order=next_order,
    )
    db.add(user_msg)
    db.flush()

    ai_result = generate_transformation(
        user_prompt=req.message,
        input_schema=t.input_schema or [],
        sample_rows=t.sample_input or [],
        conversation_history=conversation_history,
    )

    assistant_msg = ConversationMessage(
        transformation_id=transform_id,
        role="assistant",
        content=ai_result["content"],
        code_block=ai_result.get("code"),
        message_order=next_order + 1,
    )
    db.add(assistant_msg)

    if ai_result["type"] == "code" and ai_result.get("code"):
        t.generated_code = ai_result["code"]
        t.status = "code_generated"
        try:
            pipeline = db.query(Pipeline).filter(Pipeline.id == project_id).first()
            save_silver_ai_generated(
                project_name=pipeline.name if pipeline else str(project_id),
                transform_name=t.name or "unnamed",
                user_query=req.message,
                code=ai_result["code"],
            )
        except Exception as save_err:
            logger.warning("Could not save generated code to disk: %s", save_err)
    elif ai_result["type"] == "clarification":
        t.status = "chatting"
    elif ai_result["type"] == "error":
        t.status = "chatting"

    t.conversation_count = next_order + 1
    db.commit()
    db.refresh(assistant_msg)

    return ChatResponse(
        type=ai_result["type"],
        content=ai_result["content"],
        code=ai_result.get("code"),
        message_id=str(assistant_msg.id),
        transformation_status=t.status,
    )


@router.post("/{project_id}/transformations/{transform_id}/clear-chat")
def clear_chat(
    project_id: UUID,
    transform_id: UUID,
    db: Session = Depends(get_db),
):
    t = _get_transformation(db, project_id, transform_id)
    db.query(ConversationMessage).filter(
        ConversationMessage.transformation_id == transform_id
    ).delete()
    t.generated_code = None
    t.confirmed_code = None
    t.sample_output = []
    t.status = "draft"
    t.conversation_count = 0
    db.commit()
    return {"message": "Chat cleared. Ready for a new conversation."}


# ============================================================================
# Code editing & Dry-run
# ============================================================================

@router.put("/{project_id}/transformations/{transform_id}/code")
def update_code(
    project_id: UUID,
    transform_id: UUID,
    req: UpdateCodeRequest,
    db: Session = Depends(get_db),
):
    t = _get_transformation(db, project_id, transform_id)
    valid, msg = validate_transform_code(req.code)
    if not valid:
        raise HTTPException(400, f"Code validation failed: {msg}")
    t.generated_code = req.code
    t.status = "code_reviewed"
    db.commit()
    try:
        pipeline = db.query(Pipeline).filter(Pipeline.id == project_id).first()
        save_code_edit(
            project_name=pipeline.name if pipeline else str(project_id),
            transform_name=t.name or "unnamed",
            code=req.code,
        )
    except Exception as save_err:
        logger.warning("Could not save edited code to disk: %s", save_err)
    return {"message": "Code updated successfully.", "validation": msg}


@router.post("/{project_id}/transformations/{transform_id}/dry-run", response_model=DryRunResponse)
def dry_run(
    project_id: UUID,
    transform_id: UUID,
    db: Session = Depends(get_db),
):
    t = _get_transformation(db, project_id, transform_id)
    code = t.generated_code
    if not code:
        raise HTTPException(400, "No code to dry-run.")

    valid, msg = validate_transform_code(code)
    if not valid:
        return DryRunResponse(success=False, error=msg, validation_message=msg)

    sample_data = t.sample_input or []
    if not sample_data and t.input_schema:
        sample_data = build_sample_rows_from_schema(t.input_schema, num_rows=10)
        t.sample_input = sample_data
        db.commit()

    try:
        pipeline = db.query(Pipeline).filter(Pipeline.id == project_id).first()
        save_silver_dry_run(
            project_name=pipeline.name if pipeline else str(project_id),
            transform_name=t.name or "unnamed",
            code=code,
        )
    except Exception as save_err:
        logger.warning("Could not save dry-run code to disk: %s", save_err)

    try:
        result = execute_dry_run(code, t.input_schema or [], sample_data)
        if result["success"]:
            t.sample_output = result["output_rows"]
            t.output_schema = result["output_schema"]
            t.status = "dry_run_passed"
            db.commit()
        return DryRunResponse(**result)
    except Exception as e:
        logger.error("Dry-run failed: %s", e, exc_info=True)
        return DryRunResponse(success=False, error=str(e), validation_message="Dry-run execution failed.")


# ============================================================================
# Confirm — Creates a versioned snapshot
# ============================================================================

@router.post("/{project_id}/transformations/{transform_id}/confirm", response_model=TransformationResponse)
def confirm_transformation(
    project_id: UUID,
    transform_id: UUID,
    req: ConfirmRequest,
    db: Session = Depends(get_db),
):
    """
    Confirm transformation code with a name.
    - If this is the first confirm → saves as version 1
    - If already confirmed and code changed → creates new version
    """
    t = _get_transformation(db, project_id, transform_id)

    valid, msg = validate_transform_code(req.code)
    if not valid:
        raise HTTPException(400, f"Code validation failed: {msg}")

    # If already confirmed and code is different → create a new version
    if t.status == "confirmed" and t.confirmed_code and t.confirmed_code.strip() != req.code.strip():
        t.is_active = False
        t.status = "archived"
        db.flush()

        new_version = t.version + 1
        new_t = SilverTransformation(
            pipeline_id=t.pipeline_id,
            name=req.name,
            description=t.description,
            generated_code=req.code,
            confirmed_code=req.code,
            input_schema=t.input_schema,
            output_schema=t.output_schema,
            sample_input=t.sample_input,
            sample_output=t.sample_output,
            status="confirmed",
            conversation_count=t.conversation_count,
            task_order=t.task_order,
            version=new_version,
            is_active=True,
        )
        db.add(new_t)
        db.commit()
        db.refresh(new_t)

        old_messages = (
            db.query(ConversationMessage)
            .filter(ConversationMessage.transformation_id == transform_id)
            .order_by(ConversationMessage.message_order)
            .all()
        )
        for msg_obj in old_messages:
            new_msg = ConversationMessage(
                transformation_id=new_t.id,
                role=msg_obj.role,
                content=msg_obj.content,
                code_block=msg_obj.code_block,
                dry_run_result=msg_obj.dry_run_result,
                message_order=msg_obj.message_order,
            )
            db.add(new_msg)
        db.commit()

        try:
            pipeline_obj = db.query(Pipeline).filter(Pipeline.id == project_id).first()
            save_silver_confirmed(
                project_name=pipeline_obj.name if pipeline_obj else str(project_id),
                transform_name=req.name,
                code=req.code,
                version=new_version,
            )
        except Exception as save_err:
            logger.warning("Could not save confirmed code to disk: %s", save_err)
        logger.info("Created version %d for transformation '%s'", new_version, req.name)
        return _to_response(new_t)
    else:
        t.name = req.name
        t.confirmed_code = req.code
        t.generated_code = req.code
        t.status = "confirmed"
        t.is_active = True
        db.commit()
        db.refresh(t)

        pipeline = db.query(Pipeline).filter(Pipeline.id == project_id).first()
        if pipeline and pipeline.status in ("bronze_ready", "schema_confirmed"):
            pipeline.status = "silver_configured"
            db.commit()

        try:
            save_silver_confirmed(
                project_name=pipeline.name if pipeline else str(project_id),
                transform_name=t.name,
                code=req.code,
                version=t.version,
            )
        except Exception as save_err:
            logger.warning("Could not save confirmed code to disk: %s", save_err)
        logger.info("Confirmed transformation '%s' v%d", t.name, t.version)
        return _to_response(t)


# ============================================================================
# Version history for a task_order
# ============================================================================

@router.get("/{project_id}/transformations/order/{task_order}/versions", response_model=List[TransformationResponse])
def get_versions(
    project_id: UUID,
    task_order: int,
    db: Session = Depends(get_db),
):
    """Get all versions of a transformation at a specific task_order."""
    versions = (
        db.query(SilverTransformation)
        .filter(
            SilverTransformation.pipeline_id == project_id,
            SilverTransformation.task_order == task_order,
        )
        .order_by(SilverTransformation.version.desc())
        .all()
    )
    return [_to_response(t) for t in versions]


# ============================================================================
# Reorder transformations
# ============================================================================

@router.put("/{project_id}/transformations/reorder")
def reorder_transformations(
    project_id: UUID,
    order: List[str],
    db: Session = Depends(get_db),
):
    """Reorder transformations. `order` is list of transformation IDs in desired sequence."""
    for idx, tid in enumerate(order, 1):
        t = db.query(SilverTransformation).filter(
            SilverTransformation.id == tid,
            SilverTransformation.pipeline_id == project_id,
        ).first()
        if t:
            t.task_order = idx
    db.commit()
    return {"message": "Reordered", "count": len(order)}


# ============================================================================
# Upload to Silver — Apply all confirmed transformations
# ============================================================================

@router.post("/{project_id}/upload-to-silver", response_model=UploadToSilverResponse)
def upload_to_silver(
    project_id: UUID,
    req: UploadToSilverRequest = UploadToSilverRequest(),
    db: Session = Depends(get_db),
):
    """
    Apply all confirmed (active) transformations in order to the bronze data
    and write the result to the Silver bucket in MinIO.
    """
    pipeline = db.query(Pipeline).filter(Pipeline.id == project_id).first()
    if not pipeline:
        raise HTTPException(404, "Project not found")

    if req.transformation_ids:
        transforms = []
        for tid in req.transformation_ids:
            t = db.query(SilverTransformation).filter(
                SilverTransformation.id == tid,
                SilverTransformation.pipeline_id == project_id,
            ).first()
            if t and t.confirmed_code:
                transforms.append(t)
    else:
        transforms = (
            db.query(SilverTransformation)
            .filter(
                SilverTransformation.pipeline_id == project_id,
                SilverTransformation.is_active == True,
                SilverTransformation.status == "confirmed",
            )
            .order_by(SilverTransformation.task_order)
            .all()
        )

    if not transforms:
        raise HTTPException(400, "No confirmed transformations to apply. Confirm at least one transformation first.")

    execution = SilverExecution(
        pipeline_id=project_id,
        transformation_ids=[str(t.id) for t in transforms],
        status="running",
    )
    db.add(execution)
    db.commit()
    db.refresh(execution)

    start_time = time.time()

    try:
        result = execute_silver_upload(
            pipeline=pipeline,
            transforms=transforms,
            db=db,
        )

        duration = time.time() - start_time
        execution.status = "completed"
        execution.input_path = result["input_path"]
        execution.output_path = result["output_path"]
        execution.input_records = result["input_records"]
        execution.output_records = result["output_records"]
        execution.duration_seconds = duration
        execution.completed_at = datetime.utcnow()
        db.commit()

        transform_results = result.get("transform_results", [])

        pipeline.status = "active"
        db.commit()

        # Auto-refresh Gold transformation schemas with new Silver output
        try:
            gold_schema, gold_samples = get_silver_schema_and_samples(db, project_id)
            gold_transforms = (
                db.query(GoldTransformation)
                .filter(GoldTransformation.pipeline_id == project_id)
                .all()
            )
            for gt in gold_transforms:
                gt.input_schema = gold_schema
                gt.sample_input = gold_samples
            if gold_transforms:
                db.commit()
                logger.info("Auto-refreshed %d Gold transformation(s) with new Silver schema", len(gold_transforms))
        except Exception as refresh_err:
            logger.warning("Could not auto-refresh Gold schemas: %s", refresh_err)

        return UploadToSilverResponse(
            success=True,
            execution_id=str(execution.id),
            input_records=result["input_records"],
            output_records=result["output_records"],
            output_path=result["output_path"],
            duration_seconds=round(duration, 2),
            transformations_applied=len(transforms),
            transform_results=transform_results,
        )

    except Exception as e:
        duration = time.time() - start_time
        execution.status = "failed"
        execution.error_message = str(e)
        execution.duration_seconds = duration
        execution.completed_at = datetime.utcnow()
        db.commit()

        logger.error("Upload to Silver failed: %s", e, exc_info=True)
        return UploadToSilverResponse(
            success=False,
            execution_id=str(execution.id),
            input_records=0,
            output_records=0,
            output_path="",
            duration_seconds=round(duration, 2),
            transformations_applied=0,
            error=str(e),
            transform_results=[],
        )


# ============================================================================
# Execution history
# ============================================================================

@router.get("/{project_id}/executions")
def list_executions(
    project_id: UUID,
    db: Session = Depends(get_db),
):
    execs = (
        db.query(SilverExecution)
        .filter(SilverExecution.pipeline_id == project_id)
        .order_by(SilverExecution.created_at.desc())
        .all()
    )
    return [
        {
            "id": str(e.id),
            "status": e.status,
            "input_records": e.input_records,
            "output_records": e.output_records,
            "output_path": e.output_path,
            "duration_seconds": e.duration_seconds,
            "transformations_applied": len(e.transformation_ids) if e.transformation_ids else 0,
            "error": e.error_message,
            "started_at": e.started_at.isoformat() if e.started_at else "",
            "completed_at": e.completed_at.isoformat() if e.completed_at else "",
        }
        for e in execs
    ]


# ============================================================================
# Silver Data Preview
# ============================================================================

@router.get("/{project_id}/preview")
def preview_silver_data(
    project_id: UUID,
    limit: int = 50,
    db: Session = Depends(get_db),
):
    """Preview sample rows from the latest Silver execution output."""
    execution = (
        db.query(SilverExecution)
        .filter(SilverExecution.pipeline_id == project_id, SilverExecution.status == "completed")
        .order_by(SilverExecution.created_at.desc())
        .first()
    )
    if not execution:
        raise HTTPException(404, "No successful Silver execution found")

    silver_path = execution.output_path
    if not silver_path:
        raise HTTPException(404, "Silver output path not available")

    try:
        result = preview_data_from_path(silver_path, limit=limit)
        result["silver_path"] = silver_path
        return result
    except Exception as e:
        raise HTTPException(500, f"Failed to preview Silver data: {str(e)}")


# ============================================================================
# Version rollback
# ============================================================================

@router.post("/{project_id}/transformations/{transform_id}/rollback", response_model=TransformationResponse)
def rollback_version(
    project_id: UUID,
    transform_id: UUID,
    db: Session = Depends(get_db),
):
    """
    Reactivate an archived version. The currently active version at the same
    task_order will be archived, and this version becomes the active one.
    """
    target = _get_transformation(db, project_id, transform_id)

    if target.is_active and target.status != "archived":
        raise HTTPException(400, "This version is already active.")

    current_active = (
        db.query(SilverTransformation)
        .filter(
            SilverTransformation.pipeline_id == project_id,
            SilverTransformation.task_order == target.task_order,
            SilverTransformation.is_active == True,
        )
        .first()
    )

    if current_active and current_active.id != target.id:
        current_active.is_active = False
        current_active.status = "archived"

    target.is_active = True
    target.status = "confirmed" if target.confirmed_code else "draft"
    db.commit()
    db.refresh(target)

    logger.info("Rolled back to version %d for task_order %d", target.version, target.task_order)
    return _to_response(target)
