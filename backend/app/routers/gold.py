"""
Gold Layer Router — AI-powered transformations on Silver data.

Mirrors the Silver router pattern:
  Create → Chat with AI → Dry-run → Confirm → Upload to Gold → Push to Postgres
"""

import logging
import time
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from backend.app.database import get_db
from backend.app.models.models import (
    Pipeline, GoldTransformation, GoldConversationMessage,
    GoldExecution, PostgresPush,
)
from backend.app.services.ai_service import generate_transformation, validate_transform_code
from backend.app.services.code_saver import (
    save_gold_ai_generated, save_gold_confirmed, save_gold_dry_run,
    save_gold_upload_pipeline, save_gold_code_edit,
)
from backend.app.services.sandbox import build_sample_rows_from_schema, execute_dry_run
from backend.app.services.gold_service import execute_gold_upload, execute_push_to_postgres
from backend.app.services.spark_utils import (
    get_silver_schema_and_samples, preview_data_from_path,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/gold", tags=["Gold"])


# ============================================================================
# Pydantic Schemas
# ============================================================================

class CreateTransformationRequest(BaseModel):
    name: str
    description: Optional[str] = None


class TransformationResponse(BaseModel):
    id: str
    pipeline_id: str
    name: str
    description: Optional[str]
    status: str
    generated_code: Optional[str]
    confirmed_code: Optional[str]
    input_schema: list
    output_schema: list = []
    sample_input: list
    sample_output: list
    conversation_count: int
    task_order: int
    version: int
    is_active: bool
    created_at: str
    updated_at: str


class ChatRequest(BaseModel):
    message: str


class ChatResponse(BaseModel):
    type: str
    content: str
    code: Optional[str]
    message_id: str
    transformation_status: str


class MessageResponse(BaseModel):
    id: str
    role: str
    content: str
    code_block: Optional[str]
    dry_run_result: Optional[dict]
    message_order: int
    created_at: str


class UpdateCodeRequest(BaseModel):
    code: str


class ConfirmRequest(BaseModel):
    name: str
    code: str


class DryRunResponse(BaseModel):
    success: bool
    output_rows: list = []
    output_schema: list = []
    row_count: int = 0
    error: Optional[str] = None
    validation_message: str = ""


class UploadToGoldRequest(BaseModel):
    transformation_ids: Optional[List[str]] = None


class UploadToGoldResponse(BaseModel):
    success: bool
    execution_id: str
    input_records: int
    output_records: int
    output_path: str
    duration_seconds: float
    transformations_applied: int
    error: Optional[str] = None
    transform_results: list = []


class PushToPostgresRequest(BaseModel):
    table_name: str
    if_exists: str = "replace"  # replace, append, fail


class PushToPostgresResponse(BaseModel):
    success: bool
    push_id: str
    table_name: str
    records_pushed: int
    duration_seconds: float
    error: Optional[str] = None


# ============================================================================
# Helpers
# ============================================================================

def _get_transformation(db: Session, project_id: UUID, transform_id: UUID) -> GoldTransformation:
    t = (
        db.query(GoldTransformation)
        .filter(
            GoldTransformation.id == transform_id,
            GoldTransformation.pipeline_id == project_id,
        )
        .first()
    )
    if not t:
        raise HTTPException(404, "Gold transformation not found")
    return t


def _to_response(t: GoldTransformation) -> TransformationResponse:
    return TransformationResponse(
        id=str(t.id),
        pipeline_id=str(t.pipeline_id),
        name=t.name,
        description=t.description,
        status=t.status,
        generated_code=t.generated_code,
        confirmed_code=t.confirmed_code,
        input_schema=t.input_schema or [],
        output_schema=t.output_schema or [],
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
# Schema refresh
# ============================================================================

@router.post("/{project_id}/refresh-schema")
def refresh_gold_schema(
    project_id: UUID,
    db: Session = Depends(get_db),
):
    """
    Re-read the latest Silver output and update input_schema / sample_input
    on every Gold transformation for this project.
    """
    pipeline = db.query(Pipeline).filter(Pipeline.id == project_id).first()
    if not pipeline:
        raise HTTPException(404, "Project not found")

    try:
        input_schema, sample_input = get_silver_schema_and_samples(db, project_id)
    except ValueError as ve:
        raise HTTPException(400, str(ve))

    transforms = (
        db.query(GoldTransformation)
        .filter(GoldTransformation.pipeline_id == project_id)
        .all()
    )
    updated = 0
    for t in transforms:
        t.input_schema = input_schema
        t.sample_input = sample_input
        updated += 1
    db.commit()
    logger.info("Refreshed Gold schema for project %s — %d transformations updated", project_id, updated)
    return {
        "updated": updated,
        "columns": [f["name"] for f in input_schema],
        "sample_rows": len(sample_input),
    }


# ============================================================================
# CRUD Endpoints
# ============================================================================

@router.post("/{project_id}/transformations", response_model=TransformationResponse)
def create_transformation(
    project_id: UUID,
    req: CreateTransformationRequest,
    db: Session = Depends(get_db),
):
    """Create a new Gold transformation for a project."""
    pipeline = db.query(Pipeline).filter(Pipeline.id == project_id).first()
    if not pipeline:
        raise HTTPException(404, "Project not found")

    try:
        input_schema, sample_input = get_silver_schema_and_samples(db, project_id)
    except ValueError as ve:
        raise HTTPException(400, str(ve))

    existing_count = (
        db.query(GoldTransformation)
        .filter(GoldTransformation.pipeline_id == project_id)
        .count()
    )

    transformation = GoldTransformation(
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

    logger.info("Created Gold transformation '%s' for project %s", req.name, project_id)
    return _to_response(transformation)


@router.get("/{project_id}/transformations", response_model=List[TransformationResponse])
def list_transformations(
    project_id: UUID,
    active_only: bool = False,
    db: Session = Depends(get_db),
):
    pipeline = db.query(Pipeline).filter(Pipeline.id == project_id).first()
    if not pipeline:
        raise HTTPException(404, "Project not found")

    q = db.query(GoldTransformation).filter(GoldTransformation.pipeline_id == project_id)
    if active_only:
        q = q.filter(GoldTransformation.is_active == True)
    transformations = q.order_by(GoldTransformation.task_order, GoldTransformation.version.desc()).all()
    return [_to_response(t) for t in transformations]


@router.get("/{project_id}/transformations/{transform_id}", response_model=TransformationResponse)
def get_transformation_endpoint(
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
        db.query(GoldConversationMessage)
        .filter(GoldConversationMessage.transformation_id == transform_id)
        .order_by(GoldConversationMessage.message_order)
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
        db.query(GoldConversationMessage)
        .filter(GoldConversationMessage.transformation_id == transform_id)
        .order_by(GoldConversationMessage.message_order)
        .all()
    )
    conversation_history = [
        {"role": m.role, "content": m.content}
        for m in existing_messages
    ]
    next_order = len(existing_messages) + 1

    user_msg = GoldConversationMessage(
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

    assistant_msg = GoldConversationMessage(
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
            save_gold_ai_generated(
                project_name=pipeline.name if pipeline else str(project_id),
                transform_name=t.name or "unnamed",
                user_query=req.message,
                code=ai_result["code"],
            )
        except Exception as save_err:
            logger.warning("Could not save generated gold code to disk: %s", save_err)
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
    db.query(GoldConversationMessage).filter(
        GoldConversationMessage.transformation_id == transform_id
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
        save_gold_code_edit(
            project_name=pipeline.name if pipeline else str(project_id),
            transform_name=t.name or "unnamed",
            code=req.code,
        )
    except Exception as save_err:
        logger.warning("Could not save edited gold code to disk: %s", save_err)
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
        save_gold_dry_run(
            project_name=pipeline.name if pipeline else str(project_id),
            transform_name=t.name or "unnamed",
            code=code,
        )
    except Exception as save_err:
        logger.warning("Could not save dry-run gold code to disk: %s", save_err)

    try:
        result = execute_dry_run(code, t.input_schema or [], sample_data)
        if result["success"]:
            t.sample_output = result["output_rows"]
            t.output_schema = result["output_schema"]
            t.status = "dry_run_passed"
            db.commit()
        return DryRunResponse(**result)
    except Exception as e:
        logger.error("Gold dry-run failed: %s", e, exc_info=True)
        return DryRunResponse(success=False, error=str(e), validation_message="Dry-run execution failed.")


# ============================================================================
# Confirm
# ============================================================================

@router.post("/{project_id}/transformations/{transform_id}/confirm", response_model=TransformationResponse)
def confirm_transformation(
    project_id: UUID,
    transform_id: UUID,
    req: ConfirmRequest,
    db: Session = Depends(get_db),
):
    t = _get_transformation(db, project_id, transform_id)

    valid, msg = validate_transform_code(req.code)
    if not valid:
        raise HTTPException(400, f"Code validation failed: {msg}")

    if t.status == "confirmed" and t.confirmed_code and t.confirmed_code.strip() != req.code.strip():
        t.is_active = False
        t.status = "archived"
        db.flush()

        new_version = t.version + 1
        new_t = GoldTransformation(
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
            db.query(GoldConversationMessage)
            .filter(GoldConversationMessage.transformation_id == transform_id)
            .order_by(GoldConversationMessage.message_order)
            .all()
        )
        for m in old_messages:
            new_msg = GoldConversationMessage(
                transformation_id=new_t.id,
                role=m.role,
                content=m.content,
                code_block=m.code_block,
                dry_run_result=m.dry_run_result,
                message_order=m.message_order,
            )
            db.add(new_msg)
        db.commit()

        try:
            pipeline_obj = db.query(Pipeline).filter(Pipeline.id == project_id).first()
            save_gold_confirmed(
                project_name=pipeline_obj.name if pipeline_obj else str(project_id),
                transform_name=req.name,
                code=req.code,
                version=new_version,
            )
        except Exception as save_err:
            logger.warning("Could not save confirmed gold code to disk: %s", save_err)
        logger.info("Created Gold version %d for transformation '%s'", new_version, req.name)
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
        if pipeline and pipeline.status in ("silver_configured", "active"):
            pipeline.status = "gold_configured"
            db.commit()

        try:
            save_gold_confirmed(
                project_name=pipeline.name if pipeline else str(project_id),
                transform_name=t.name,
                code=req.code,
                version=t.version,
            )
        except Exception as save_err:
            logger.warning("Could not save confirmed gold code to disk: %s", save_err)
        logger.info("Confirmed Gold transformation '%s' v%d", t.name, t.version)
        return _to_response(t)


# ============================================================================
# Version history
# ============================================================================

@router.get("/{project_id}/transformations/order/{task_order}/versions", response_model=List[TransformationResponse])
def get_versions(
    project_id: UUID,
    task_order: int,
    db: Session = Depends(get_db),
):
    versions = (
        db.query(GoldTransformation)
        .filter(
            GoldTransformation.pipeline_id == project_id,
            GoldTransformation.task_order == task_order,
        )
        .order_by(GoldTransformation.version.desc())
        .all()
    )
    return [_to_response(t) for t in versions]


# ============================================================================
# Reorder
# ============================================================================

@router.put("/{project_id}/transformations/reorder")
def reorder_transformations(
    project_id: UUID,
    order: List[str],
    db: Session = Depends(get_db),
):
    for idx, tid in enumerate(order, 1):
        t = db.query(GoldTransformation).filter(
            GoldTransformation.id == tid,
            GoldTransformation.pipeline_id == project_id,
        ).first()
        if t:
            t.task_order = idx
    db.commit()
    return {"message": "Reordered", "count": len(order)}


# ============================================================================
# Upload to Gold
# ============================================================================

@router.post("/{project_id}/upload-to-gold", response_model=UploadToGoldResponse)
def upload_to_gold(
    project_id: UUID,
    req: UploadToGoldRequest = UploadToGoldRequest(),
    db: Session = Depends(get_db),
):
    """
    Apply all confirmed Gold transformations in order to the Silver data
    and write the result to the Gold bucket in MinIO.
    """
    pipeline = db.query(Pipeline).filter(Pipeline.id == project_id).first()
    if not pipeline:
        raise HTTPException(404, "Project not found")

    if req.transformation_ids:
        transforms = []
        for tid in req.transformation_ids:
            t = db.query(GoldTransformation).filter(
                GoldTransformation.id == tid,
                GoldTransformation.pipeline_id == project_id,
            ).first()
            if t and t.confirmed_code:
                transforms.append(t)
    else:
        transforms = (
            db.query(GoldTransformation)
            .filter(
                GoldTransformation.pipeline_id == project_id,
                GoldTransformation.is_active == True,
                GoldTransformation.status == "confirmed",
            )
            .order_by(GoldTransformation.task_order)
            .all()
        )

    if not transforms:
        raise HTTPException(400, "No confirmed Gold transformations to apply.")

    execution = GoldExecution(
        pipeline_id=project_id,
        transformation_ids=[str(t.id) for t in transforms],
        status="running",
    )
    db.add(execution)
    db.commit()
    db.refresh(execution)

    start_time = time.time()

    try:
        result = execute_gold_upload(
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

        pipeline.status = "gold_ready"
        db.commit()

        return UploadToGoldResponse(
            success=True,
            execution_id=str(execution.id),
            input_records=result["input_records"],
            output_records=result["output_records"],
            output_path=result["output_path"],
            duration_seconds=round(duration, 2),
            transformations_applied=len(transforms),
            transform_results=result.get("transform_results", []),
        )

    except Exception as e:
        duration = time.time() - start_time
        db.rollback()
        execution.status = "failed"
        execution.error_message = str(e)
        execution.duration_seconds = duration
        execution.completed_at = datetime.utcnow()
        db.commit()

        logger.error("Upload to Gold failed: %s", e, exc_info=True)
        return UploadToGoldResponse(
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
# Push to Postgres
# ============================================================================

@router.post("/{project_id}/push-to-postgres", response_model=PushToPostgresResponse)
def push_to_postgres(
    project_id: UUID,
    req: PushToPostgresRequest,
    db: Session = Depends(get_db),
):
    """
    Read the latest Gold data from MinIO and push it to a Postgres table.
    """
    pipeline = db.query(Pipeline).filter(Pipeline.id == project_id).first()
    if not pipeline:
        raise HTTPException(404, "Project not found")

    latest_exec = (
        db.query(GoldExecution)
        .filter(GoldExecution.pipeline_id == project_id, GoldExecution.status == "completed")
        .order_by(GoldExecution.created_at.desc())
        .first()
    )
    if not latest_exec or not latest_exec.output_path:
        raise HTTPException(400, "No completed Gold execution found. Upload to Gold first.")

    push_record = PostgresPush(
        pipeline_id=project_id,
        gold_execution_id=latest_exec.id,
        table_name=req.table_name,
        status="running",
    )
    db.add(push_record)
    db.commit()
    db.refresh(push_record)

    start_time = time.time()

    try:
        records = execute_push_to_postgres(
            gold_path=latest_exec.output_path,
            table_name=req.table_name,
            if_exists=req.if_exists,
        )

        duration = time.time() - start_time
        push_record.status = "completed"
        push_record.records_pushed = records
        push_record.duration_seconds = duration
        push_record.completed_at = datetime.utcnow()
        db.commit()

        return PushToPostgresResponse(
            success=True,
            push_id=str(push_record.id),
            table_name=req.table_name,
            records_pushed=records,
            duration_seconds=round(duration, 2),
        )

    except Exception as e:
        duration = time.time() - start_time
        push_record.status = "failed"
        push_record.error_message = str(e)
        push_record.duration_seconds = duration
        push_record.completed_at = datetime.utcnow()
        db.commit()

        logger.error("Push to Postgres failed: %s", e, exc_info=True)
        return PushToPostgresResponse(
            success=False,
            push_id=str(push_record.id),
            table_name=req.table_name,
            records_pushed=0,
            duration_seconds=round(duration, 2),
            error=str(e),
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
        db.query(GoldExecution)
        .filter(GoldExecution.pipeline_id == project_id)
        .order_by(GoldExecution.created_at.desc())
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


@router.get("/{project_id}/postgres-pushes")
def list_postgres_pushes(
    project_id: UUID,
    db: Session = Depends(get_db),
):
    pushes = (
        db.query(PostgresPush)
        .filter(PostgresPush.pipeline_id == project_id)
        .order_by(PostgresPush.created_at.desc())
        .all()
    )
    return [
        {
            "id": str(p.id),
            "table_name": p.table_name,
            "status": p.status,
            "records_pushed": p.records_pushed,
            "duration_seconds": p.duration_seconds,
            "error": p.error_message,
            "created_at": p.created_at.isoformat() if p.created_at else "",
            "completed_at": p.completed_at.isoformat() if p.completed_at else "",
        }
        for p in pushes
    ]


# ============================================================================
# Gold Data Preview
# ============================================================================

@router.get("/{project_id}/preview")
def preview_gold_data(
    project_id: UUID,
    limit: int = 50,
    db: Session = Depends(get_db),
):
    """Preview sample rows from the latest Gold execution output."""
    execution = (
        db.query(GoldExecution)
        .filter(GoldExecution.pipeline_id == project_id, GoldExecution.status == "completed")
        .order_by(GoldExecution.created_at.desc())
        .first()
    )
    if not execution:
        raise HTTPException(404, "No successful Gold execution found")

    gold_path = execution.output_path
    if not gold_path:
        raise HTTPException(404, "Gold output path not available")

    try:
        result = preview_data_from_path(gold_path, limit=limit)
        result["gold_path"] = gold_path
        return result
    except Exception as e:
        raise HTTPException(500, f"Failed to preview Gold data: {str(e)}")


# ============================================================================
# Version rollback
# ============================================================================

@router.post("/{project_id}/transformations/{transform_id}/rollback", response_model=TransformationResponse)
def rollback_version(
    project_id: UUID,
    transform_id: UUID,
    db: Session = Depends(get_db),
):
    target = _get_transformation(db, project_id, transform_id)

    if target.is_active and target.status != "archived":
        raise HTTPException(400, "This version is already active.")

    current_active = (
        db.query(GoldTransformation)
        .filter(
            GoldTransformation.pipeline_id == project_id,
            GoldTransformation.task_order == target.task_order,
            GoldTransformation.is_active == True,
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

    logger.info("Rolled back Gold to version %d for task_order %d", target.version, target.task_order)
    return _to_response(target)
