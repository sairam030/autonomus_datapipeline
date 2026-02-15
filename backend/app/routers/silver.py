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
    SilverExecution, BronzeIngestion,
)
from backend.app.services.ai_service import generate_transformation, validate_transform_code
from backend.app.services.code_saver import (
    save_silver_ai_generated, save_silver_confirmed,
    save_silver_dry_run, save_silver_upload_pipeline, save_code_edit,
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

def _build_sample_rows_from_schema(input_schema: list, num_rows: int = 10) -> list:
    """Synthesize sample rows from schema's sample_values."""
    if not input_schema:
        return []
    TYPE_CASTERS = {
        "integer": lambda v: int(v) if v is not None else None,
        "long": lambda v: int(v) if v is not None else None,
        "float": lambda v: float(v) if v is not None else None,
        "double": lambda v: float(v) if v is not None else None,
        "boolean": lambda v: (str(v).lower() in ("true", "1", "yes")) if v is not None else None,
        "string": lambda v: str(v) if v is not None else None,
    }
    rows = []
    for i in range(num_rows):
        row = {}
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
            caster = TYPE_CASTERS.get(dtype, TYPE_CASTERS["string"])
            try:
                row[name] = caster(raw)
            except (ValueError, TypeError):
                row[name] = raw
        rows.append(row)
    return rows


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
    sample_input = _build_sample_rows_from_schema(input_schema, num_rows=10)

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
    t = _get_transformation(db, project_id, transform_id)
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
        # Save to local filesystem for reference
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
    # Save manual edit to disk
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
        sample_data = _build_sample_rows_from_schema(t.input_schema, num_rows=10)
        t.sample_input = sample_data
        db.commit()

    # Save dry-run code to disk
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
        result = _execute_dry_run(code, t.input_schema or [], sample_data)
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
        # Archive the current one
        t.is_active = False
        t.status = "archived"
        db.flush()

        # Create new version
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

        # Copy conversation messages to new transformation
        old_messages = (
            db.query(ConversationMessage)
            .filter(ConversationMessage.transformation_id == transform_id)
            .order_by(ConversationMessage.message_order)
            .all()
        )
        for msg in old_messages:
            new_msg = ConversationMessage(
                transformation_id=new_t.id,
                role=msg.role,
                content=msg.content,
                code_block=msg.code_block,
                dry_run_result=msg.dry_run_result,
                message_order=msg.message_order,
            )
            db.add(new_msg)
        db.commit()

        # Save new version code to disk
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
        # First confirm or re-confirm with same code
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

        # Save confirmed code to disk
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

    # Get confirmed active transformations in order
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

    # Create execution record
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
        result = _execute_silver_upload(
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

        # Update pipeline status
        pipeline.status = "active"
        db.commit()

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
# Internal: PySpark execution
# ============================================================================
# ============================================================================
# Safe exec() sandbox
# ============================================================================

_SAFE_BUILTINS = {
    # Allowed builtins for transformation code — no file I/O, no eval/exec,
    # no import manipulation, no process control.
    k: v for k, v in __builtins__.items()  # type: ignore[union-attr]
    if k not in {
        'eval', 'exec', 'compile', '__import__', 'open',
        'breakpoint', 'exit', 'quit', 'input',
        'globals', 'locals', 'vars', 'dir',
        'getattr', 'setattr', 'delattr',
        'memoryview', 'classmethod', 'staticmethod',
    }
} if isinstance(__builtins__, dict) else {
    k: getattr(__builtins__, k)
    for k in dir(__builtins__)
    if not k.startswith('_') and k not in {
        'eval', 'exec', 'compile', '__import__', 'open',
        'breakpoint', 'exit', 'quit', 'input',
        'globals', 'locals', 'vars', 'dir',
        'getattr', 'setattr', 'delattr',
        'memoryview', 'classmethod', 'staticmethod',
    }
}


def _safe_import(name, *args, **kwargs):
    """Only allow importing data-processing libraries."""
    ALLOWED_MODULES = {
        'pyspark', 'pyspark.sql', 'pyspark.sql.functions',
        'pyspark.sql.types', 'pyspark.sql.window',
        'math', 'datetime', 'decimal', 'json', 're',
        'collections', 'functools', 'itertools', 'operator',
        'typing', 'string', 'hashlib', 'uuid',
    }
    top_level = name.split('.')[0]
    if top_level not in {m.split('.')[0] for m in ALLOWED_MODULES} and name not in ALLOWED_MODULES:
        raise ImportError(f"Import of '{name}' is not allowed in transformation code.")
    return __builtins__['__import__'](name, *args, **kwargs) if isinstance(__builtins__, dict) \
        else __import__(name, *args, **kwargs)


def _build_safe_exec_globals() -> dict:
    """Build a restricted globals dict for exec() to sandbox user/AI code."""
    safe = {'__builtins__': {**_SAFE_BUILTINS, '__import__': _safe_import}}
    return safe



# ============================================================================
# Safe exec() sandbox
# ============================================================================

_SAFE_BUILTINS = {
    # Allowed builtins for transformation code — no file I/O, no eval/exec,
    # no import manipulation, no process control.
    k: v for k, v in __builtins__.items()  # type: ignore[union-attr]
    if k not in {
        'eval', 'exec', 'compile', '__import__', 'open',
        'breakpoint', 'exit', 'quit', 'input',
        'globals', 'locals', 'vars', 'dir',
        'getattr', 'setattr', 'delattr',
        'memoryview', 'classmethod', 'staticmethod',
    }
} if isinstance(__builtins__, dict) else {
    k: getattr(__builtins__, k)
    for k in dir(__builtins__)
    if not k.startswith('_') and k not in {
        'eval', 'exec', 'compile', '__import__', 'open',
        'breakpoint', 'exit', 'quit', 'input',
        'globals', 'locals', 'vars', 'dir',
        'getattr', 'setattr', 'delattr',
        'memoryview', 'classmethod', 'staticmethod',
    }
}


def _safe_import(name, *args, **kwargs):
    """Only allow importing data-processing libraries."""
    ALLOWED_MODULES = {
        'pyspark', 'pyspark.sql', 'pyspark.sql.functions',
        'pyspark.sql.types', 'pyspark.sql.window',
        'math', 'datetime', 'decimal', 'json', 're',
        'collections', 'functools', 'itertools', 'operator',
        'typing', 'string', 'hashlib', 'uuid',
    }
    top_level = name.split('.')[0]
    if top_level not in {m.split('.')[0] for m in ALLOWED_MODULES} and name not in ALLOWED_MODULES:
        raise ImportError(f"Import of '{name}' is not allowed in transformation code.")
    return __builtins__['__import__'](name, *args, **kwargs) if isinstance(__builtins__, dict) \
        else __import__(name, *args, **kwargs)


def _build_safe_exec_globals() -> dict:
    """Build a restricted globals dict for exec() to sandbox user/AI code."""
    safe = {'__builtins__': {**_SAFE_BUILTINS, '__import__': _safe_import}}
    return safe




def _execute_dry_run(code: str, input_schema: list, sample_rows: list) -> dict:
    """Execute transform code on sample data using PySpark."""
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, LongType

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

        fields = []
        for f in input_schema:
            name = f.get("name", "col")
            dtype = f.get("detected_type", f.get("type", "string")).lower()
            spark_type = TYPE_MAP.get(dtype, StringType())
            nullable = f.get("nullable", True)
            fields.append(StructField(name, spark_type, nullable))
        schema = StructType(fields) if fields else None

        if sample_rows:
            rows = sample_rows[:10]
            df = spark.createDataFrame(rows, schema=schema) if schema else spark.createDataFrame(rows)
        elif schema:
            df = spark.createDataFrame([], schema=schema)
        else:
            return {"success": False, "output_rows": [], "output_schema": [], "row_count": 0,
                    "error": "No sample data and no schema.", "validation_message": "Cannot create test DataFrame."}

        exec_globals = _build_safe_exec_globals()
        exec(code, exec_globals)
        transform_fn = exec_globals.get("transform")
        if not transform_fn:
            return {"success": False, "output_rows": [], "output_schema": [], "row_count": 0,
                    "error": "`transform` function not found.", "validation_message": "Code must define `def transform(df, spark):`"}

        result_df = transform_fn(df, spark)
        output_rows = [row.asDict() for row in result_df.collect()]
        output_schema = [{"name": f.name, "type": str(f.dataType), "nullable": f.nullable} for f in result_df.schema.fields]

        return {"success": True, "output_rows": output_rows, "output_schema": output_schema,
                "row_count": len(output_rows), "error": None,
                "validation_message": f"Dry-run successful: {len(output_rows)} rows, {len(output_schema)} columns."}

    except Exception as e:
        return {"success": False, "output_rows": [], "output_schema": [], "row_count": 0,
                "error": str(e), "validation_message": f"Dry-run failed: {str(e)}"}
    finally:
        if spark:
            spark.stop()


def _execute_silver_upload(pipeline, transforms: list, db: Session) -> dict:
    """
    Read bronze data from MinIO, apply all transformations sequentially,
    write result to silver bucket.
    """
    import os
    from pyspark.sql import SparkSession

    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_access = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    minio_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

    import re as _re
    silver_bucket = "silver"
    pipeline_slug = _re.sub(r"[^a-z0-9]+", "_", pipeline.name.lower()).strip("_")
    silver_path = f"s3a://{silver_bucket}/{pipeline_slug}/silver/"

    # Bronze path will be read from the latest ingestion record (set below)
    bronze_path = None

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
            .master("local[*]")
            .appName(f"silver_upload_{pipeline_slug}")
            .config("spark.driver.memory", "1g")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", minio_access)
            .config("spark.hadoop.fs.s3a.secret.key", minio_secret)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )

        # Ensure silver bucket exists
        import boto3
        s3 = boto3.client(
            "s3",
            endpoint_url=minio_endpoint,
            aws_access_key_id=minio_access,
            aws_secret_access_key=minio_secret,
        )
        try:
            s3.head_bucket(Bucket=silver_bucket)
        except Exception:
            s3.create_bucket(Bucket=silver_bucket)

        # ---------- locate the latest bronze version path ----------
        latest_ingestion = (
            db.query(BronzeIngestion)
            .filter(BronzeIngestion.pipeline_id == pipeline.id)
            .order_by(BronzeIngestion.created_at.desc())
            .first()
        )
        if latest_ingestion and latest_ingestion.bronze_path:
            bronze_path = latest_ingestion.bronze_path

        if not bronze_path:
            raise ValueError('No Bronze ingestion found. Run Bronze ingestion first.')

        # ---------- read bronze CSV data ----------
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(bronze_path))

        input_count = df.count()

        # Save the combined pipeline code to disk for reference
        try:
            save_silver_upload_pipeline(
                project_name=pipeline.name,
                transforms=[
                    {"name": t.name, "code": t.confirmed_code, "version": t.version}
                    for t in transforms
                ],
                bronze_path=bronze_path,
                silver_path=silver_path,
            )
        except Exception as save_err:
            logger.warning("Could not save upload pipeline code to disk: %s", save_err)

        # Apply each transformation in order — track per-transform results
        transform_results = []
        for t in transforms:
            t_start = time.time()
            try:
                exec_globals = _build_safe_exec_globals()
                exec(t.confirmed_code, exec_globals)
                transform_fn = exec_globals.get("transform")
                if not transform_fn:
                    raise ValueError(f"Transform '{t.name}' (v{t.version}) has no `transform` function")
                df = transform_fn(df, spark)
                transform_results.append({
                    "id": str(t.id),
                    "name": t.name,
                    "version": t.version,
                    "status": "success",
                    "duration_seconds": round(time.time() - t_start, 2),
                })
            except Exception as te:
                transform_results.append({
                    "id": str(t.id),
                    "name": t.name,
                    "version": t.version,
                    "status": "failed",
                    "error": str(te),
                    "duration_seconds": round(time.time() - t_start, 2),
                })
                raise ValueError(
                    f"Transform '{t.name}' (v{t.version}) failed: {str(te)}"
                ) from te

        output_count = df.count()

        # Write to silver bucket as CSV
        df.write.mode("overwrite").option("header", "true").csv(silver_path)

        return {
            "input_path": bronze_path,
            "output_path": silver_path,
            "input_records": input_count,
            "output_records": output_count,
            "transform_results": transform_results,
        }

    finally:
        if spark:
            spark.stop()


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
    from pyspark.sql import SparkSession
    import os

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

    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_access = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    minio_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

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
            .appName("silver_preview")
            .config("spark.driver.memory", "512m")
            .config("spark.ui.enabled", "false")
            .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", minio_access)
            .config("spark.hadoop.fs.s3a.secret.key", minio_secret)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )

        df = spark.read.option("header", "true").option("inferSchema", "true").csv(silver_path)
        total_count = df.count()
        schema_info = [{"name": f.name, "type": str(f.dataType), "nullable": f.nullable} for f in df.schema.fields]
        sample_rows = [row.asDict() for row in df.limit(limit).collect()]

        for row in sample_rows:
            for k, v in row.items():
                if v is not None and not isinstance(v, (str, int, float, bool)):
                    row[k] = str(v)

        return {
            "total_records": total_count,
            "columns": schema_info,
            "rows": sample_rows,
            "silver_path": silver_path,
            "preview_count": len(sample_rows),
        }
    except Exception as e:
        raise HTTPException(500, f"Failed to preview Silver data: {str(e)}")
    finally:
        if spark:
            spark.stop()


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

    # Archive the currently active version at same task_order
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

    # Reactivate the target
    target.is_active = True
    target.status = "confirmed" if target.confirmed_code else "draft"
    db.commit()
    db.refresh(target)

    logger.info("Rolled back to version %d for task_order %d", target.version, target.task_order)
    return _to_response(target)
