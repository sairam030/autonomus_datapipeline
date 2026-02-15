"""
Silver Transformation Router — AI-driven enrichment endpoints.

Supports conversational AI flow:
  1. Create a transformation (or start a new chat)
  2. Send chat messages → AI responds with clarification or code
  3. Edit code in editor
  4. Dry-run on sample data
  5. Confirm and save
"""

import logging
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.app.database import get_db
from backend.app.models.models import (
    Pipeline, SchemaRegistry, SilverTransformation, ConversationMessage,
)
from backend.app.services.ai_service import generate_transformation, validate_transform_code

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/silver", tags=["Silver Transformations"])


# ============================================================================
# Request / Response schemas
# ============================================================================

class CreateTransformationRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, description="User's prompt or follow-up message")


class UpdateCodeRequest(BaseModel):
    code: str = Field(..., description="User-edited PySpark code")


class ConfirmRequest(BaseModel):
    code: str = Field(..., description="Final confirmed PySpark code")


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
    created_at: str
    updated_at: str

    class Config:
        from_attributes = True


class ChatResponse(BaseModel):
    type: str           # "clarification", "code", "error"
    content: str        # Full AI response text
    code: Optional[str] = None  # Extracted code block
    message_id: str     # ID of the saved assistant message
    transformation_status: str


class DryRunResponse(BaseModel):
    success: bool
    output_rows: list = []
    output_schema: list = []
    row_count: int = 0
    error: Optional[str] = None
    validation_message: str = ""


# ============================================================================
# Endpoints
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

    # Get the latest confirmed schema for input context
    schema_reg = (
        db.query(SchemaRegistry)
        .filter(SchemaRegistry.pipeline_id == project_id)
        .order_by(SchemaRegistry.version.desc())
        .first()
    )
    input_schema = schema_reg.fields if schema_reg else []

    # Get sample data from schema detection (stored in schema_registry compatible_files or sample)
    sample_input = []
    if schema_reg and schema_reg.compatible_files:
        # Try to extract sample rows from detection metadata
        # For now, store empty — will be populated when user triggers chat
        pass

    # Determine task_order
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
    )
    db.add(transformation)
    db.commit()
    db.refresh(transformation)

    logger.info("Created Silver transformation '%s' for project %s", req.name, project_id)
    return _to_response(transformation)


@router.get("/{project_id}/transformations", response_model=List[TransformationResponse])
def list_transformations(
    project_id: UUID,
    db: Session = Depends(get_db),
):
    """List all Silver transformations for a project."""
    pipeline = db.query(Pipeline).filter(Pipeline.id == project_id).first()
    if not pipeline:
        raise HTTPException(404, "Project not found")

    transformations = (
        db.query(SilverTransformation)
        .filter(SilverTransformation.pipeline_id == project_id)
        .order_by(SilverTransformation.task_order)
        .all()
    )
    return [_to_response(t) for t in transformations]


@router.get("/{project_id}/transformations/{transform_id}", response_model=TransformationResponse)
def get_transformation(
    project_id: UUID,
    transform_id: UUID,
    db: Session = Depends(get_db),
):
    """Get a specific Silver transformation."""
    t = _get_transformation(db, project_id, transform_id)
    return _to_response(t)


@router.delete("/{project_id}/transformations/{transform_id}")
def delete_transformation(
    project_id: UUID,
    transform_id: UUID,
    db: Session = Depends(get_db),
):
    """Delete a Silver transformation and its conversation."""
    t = _get_transformation(db, project_id, transform_id)
    db.delete(t)
    db.commit()
    return {"deleted": str(transform_id)}


# --------------------------------------------------------------------------
# Chat / Conversation
# --------------------------------------------------------------------------

@router.get("/{project_id}/transformations/{transform_id}/messages", response_model=List[MessageResponse])
def get_messages(
    project_id: UUID,
    transform_id: UUID,
    db: Session = Depends(get_db),
):
    """Get all conversation messages for a transformation."""
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
    """
    Send a user message and get an AI response.
    The AI may respond with clarifying questions or generated code.
    Full conversation history is sent as context for continuity.
    """
    t = _get_transformation(db, project_id, transform_id)

    # Build conversation history from DB
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

    # Determine next message order
    next_order = len(existing_messages) + 1

    # Save user message
    user_msg = ConversationMessage(
        transformation_id=transform_id,
        role="user",
        content=req.message,
        message_order=next_order,
    )
    db.add(user_msg)
    db.flush()

    # Call Gemini AI
    ai_result = generate_transformation(
        user_prompt=req.message,
        input_schema=t.input_schema or [],
        sample_rows=t.sample_input or [],
        conversation_history=conversation_history,
    )

    # Save assistant message
    assistant_msg = ConversationMessage(
        transformation_id=transform_id,
        role="assistant",
        content=ai_result["content"],
        code_block=ai_result.get("code"),
        message_order=next_order + 1,
    )
    db.add(assistant_msg)

    # Update transformation status and code
    if ai_result["type"] == "code" and ai_result.get("code"):
        t.generated_code = ai_result["code"]
        t.status = "code_generated"
    elif ai_result["type"] == "clarification":
        t.status = "chatting"
    elif ai_result["type"] == "error":
        t.status = "chatting"  # keep chatting on error

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
    """Clear all conversation messages (start a new chat for different transformation)."""
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


# --------------------------------------------------------------------------
# Code editing & validation
# --------------------------------------------------------------------------

@router.put("/{project_id}/transformations/{transform_id}/code")
def update_code(
    project_id: UUID,
    transform_id: UUID,
    req: UpdateCodeRequest,
    db: Session = Depends(get_db),
):
    """Update the generated code (user edited in Monaco editor)."""
    t = _get_transformation(db, project_id, transform_id)

    # Validate
    valid, msg = validate_transform_code(req.code)
    if not valid:
        raise HTTPException(400, f"Code validation failed: {msg}")

    t.generated_code = req.code
    t.status = "code_reviewed"
    db.commit()

    return {"message": "Code updated successfully.", "validation": msg}


@router.post("/{project_id}/transformations/{transform_id}/dry-run", response_model=DryRunResponse)
def dry_run(
    project_id: UUID,
    transform_id: UUID,
    db: Session = Depends(get_db),
):
    """
    Execute the transform function on sample data and return the result.
    Uses PySpark locally with a small sample to validate the code works.
    """
    t = _get_transformation(db, project_id, transform_id)

    code = t.generated_code
    if not code:
        raise HTTPException(400, "No code to dry-run. Generate or write code first.")

    # Validate first
    valid, msg = validate_transform_code(code)
    if not valid:
        return DryRunResponse(success=False, error=msg, validation_message=msg)

    # Execute with PySpark on sample data
    try:
        result = _execute_dry_run(code, t.input_schema or [], t.sample_input or [])
        if result["success"]:
            t.sample_output = result["output_rows"]
            t.output_schema = result["output_schema"]
            t.status = "dry_run_passed"
            db.commit()
        return DryRunResponse(**result)
    except Exception as e:
        logger.error("Dry-run failed: %s", e, exc_info=True)
        return DryRunResponse(
            success=False,
            error=str(e),
            validation_message="Dry-run execution failed.",
        )


# --------------------------------------------------------------------------
# Confirm transformation
# --------------------------------------------------------------------------

@router.post("/{project_id}/transformations/{transform_id}/confirm", response_model=TransformationResponse)
def confirm_transformation(
    project_id: UUID,
    transform_id: UUID,
    req: ConfirmRequest,
    db: Session = Depends(get_db),
):
    """Confirm the final transformation code. Ready for DAG generation."""
    t = _get_transformation(db, project_id, transform_id)

    # Validate
    valid, msg = validate_transform_code(req.code)
    if not valid:
        raise HTTPException(400, f"Code validation failed: {msg}")

    t.confirmed_code = req.code
    t.generated_code = req.code
    t.status = "confirmed"
    db.commit()
    db.refresh(t)

    # Update pipeline status
    pipeline = db.query(Pipeline).filter(Pipeline.id == project_id).first()
    if pipeline and pipeline.status in ("bronze_ready", "schema_confirmed"):
        pipeline.status = "silver_configured"
        db.commit()

    logger.info("Confirmed Silver transformation '%s' for project %s", t.name, project_id)
    return _to_response(t)


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
        created_at=t.created_at.isoformat() if t.created_at else "",
        updated_at=t.updated_at.isoformat() if t.updated_at else "",
    )


def _execute_dry_run(code: str, input_schema: list, sample_rows: list) -> dict:
    """
    Execute the transform code on sample data using PySpark.
    Returns output rows, schema, and success status.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, LongType

    # Type mapping
    TYPE_MAP = {
        "string": StringType(),
        "integer": IntegerType(),
        "long": LongType(),
        "float": FloatType(),
        "double": FloatType(),
        "boolean": BooleanType(),
    }

    spark = None
    try:
        spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("dry_run")
            .config("spark.driver.memory", "512m")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

        # Build schema from input_schema
        fields = []
        for f in input_schema:
            name = f.get("name", "col")
            dtype = f.get("detected_type", f.get("type", "string")).lower()
            spark_type = TYPE_MAP.get(dtype, StringType())
            nullable = f.get("nullable", True)
            fields.append(StructField(name, spark_type, nullable))

        schema = StructType(fields) if fields else None

        # Create DataFrame from sample data
        if sample_rows and len(sample_rows) > 0:
            # Use only first 10 rows for dry-run
            rows = sample_rows[:10]
            if schema:
                df = spark.createDataFrame(rows, schema=schema)
            else:
                df = spark.createDataFrame(rows)
        else:
            # No sample data — create empty DataFrame with schema
            if schema:
                df = spark.createDataFrame([], schema=schema)
            else:
                return {
                    "success": False,
                    "output_rows": [],
                    "output_schema": [],
                    "row_count": 0,
                    "error": "No sample data and no schema available for dry-run.",
                    "validation_message": "Cannot create test DataFrame.",
                }

        # Execute the transform
        exec_globals = {}
        exec(code, exec_globals)
        transform_fn = exec_globals.get("transform")
        if not transform_fn:
            return {
                "success": False,
                "output_rows": [],
                "output_schema": [],
                "row_count": 0,
                "error": "`transform` function not found in code.",
                "validation_message": "Code must define `def transform(df, spark):`",
            }

        result_df = transform_fn(df, spark)

        # Collect results
        output_rows = [row.asDict() for row in result_df.collect()]
        output_schema = [
            {"name": f.name, "type": str(f.dataType), "nullable": f.nullable}
            for f in result_df.schema.fields
        ]

        return {
            "success": True,
            "output_rows": output_rows,
            "output_schema": output_schema,
            "row_count": len(output_rows),
            "error": None,
            "validation_message": f"Dry-run successful: {len(output_rows)} rows, {len(output_schema)} columns.",
        }

    except Exception as e:
        return {
            "success": False,
            "output_rows": [],
            "output_schema": [],
            "row_count": 0,
            "error": str(e),
            "validation_message": f"Dry-run failed: {str(e)}",
        }
    finally:
        if spark:
            spark.stop()
