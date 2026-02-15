"""
File upload API routes.

Users upload data files (CSV, JSON, Parquet) via the UI.
Files are saved inside the container at /data/pipeline/<pipeline_id>/
Schema detection then scans that directory.
"""

import os
import logging
import shutil
from uuid import UUID
from typing import List

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from sqlalchemy.orm import Session

from backend.app.database import get_db
from backend.app.models.models import Pipeline, DataSource
from backend.app.config import get_settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/upload", tags=["File Upload"])

ALLOWED_EXTENSIONS = {
    "csv": [".csv"],
    "json": [".json", ".jsonl"],
    "parquet": [".parquet", ".pq"],
}


def _get_upload_dir(pipeline_id: UUID) -> str:
    """Get the upload directory for a pipeline, create if needed."""
    settings = get_settings()
    upload_dir = os.path.join(settings.data_mount_path, str(pipeline_id))
    os.makedirs(upload_dir, exist_ok=True)
    return upload_dir


@router.post("/{pipeline_id}/files")
async def upload_files(
    pipeline_id: UUID,
    files: List[UploadFile] = File(...),
    db: Session = Depends(get_db),
):
    """
    Upload one or more data files for a pipeline.
    Files are saved to /data/pipeline/<pipeline_id>/
    Returns list of uploaded files with their paths.
    """
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    if not pipeline:
        raise HTTPException(404, "Pipeline not found")

    upload_dir = _get_upload_dir(pipeline_id)
    uploaded = []
    errors = []

    for file in files:
        if not file.filename:
            errors.append({"filename": "unknown", "error": "No filename"})
            continue

        # Validate extension
        ext = os.path.splitext(file.filename)[1].lower()
        all_allowed = [e for exts in ALLOWED_EXTENSIONS.values() for e in exts]
        if ext not in all_allowed:
            errors.append({
                "filename": file.filename,
                "error": f"Unsupported file type '{ext}'. Allowed: {all_allowed}"
            })
            continue

        # Save file
        file_path = os.path.join(upload_dir, file.filename)
        try:
            with open(file_path, "wb") as f:
                shutil.copyfileobj(file.file, f)

            file_size = os.path.getsize(file_path)
            uploaded.append({
                "filename": file.filename,
                "path": file_path,
                "size_bytes": file_size,
            })
            logger.info(f"Uploaded {file.filename} ({file_size} bytes) → {file_path}")

        except Exception as e:
            errors.append({"filename": file.filename, "error": str(e)})
            logger.error(f"Failed to save {file.filename}: {e}")

    # Auto-detect format from first uploaded file
    detected_format = None
    if uploaded:
        first_ext = os.path.splitext(uploaded[0]["filename"])[1].lower()
        for fmt, exts in ALLOWED_EXTENSIONS.items():
            if first_ext in exts:
                detected_format = fmt
                break

    return {
        "pipeline_id": str(pipeline_id),
        "upload_dir": upload_dir,
        "uploaded_files": uploaded,
        "errors": errors,
        "total_uploaded": len(uploaded),
        "total_errors": len(errors),
        "detected_format": detected_format,
    }


@router.get("/{pipeline_id}/files")
def list_uploaded_files(
    pipeline_id: UUID,
    db: Session = Depends(get_db),
):
    """List all uploaded files for a pipeline."""
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    if not pipeline:
        raise HTTPException(404, "Pipeline not found")

    upload_dir = _get_upload_dir(pipeline_id)
    files = []

    if os.path.isdir(upload_dir):
        for filename in sorted(os.listdir(upload_dir)):
            file_path = os.path.join(upload_dir, filename)
            if os.path.isfile(file_path):
                files.append({
                    "filename": filename,
                    "path": file_path,
                    "size_bytes": os.path.getsize(file_path),
                })

    return {
        "pipeline_id": str(pipeline_id),
        "upload_dir": upload_dir,
        "files": files,
        "total_files": len(files),
    }


@router.delete("/{pipeline_id}/files/{filename}")
def delete_uploaded_file(
    pipeline_id: UUID,
    filename: str,
    db: Session = Depends(get_db),
):
    """Delete a specific uploaded file."""
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    if not pipeline:
        raise HTTPException(404, "Pipeline not found")

    upload_dir = _get_upload_dir(pipeline_id)
    file_path = os.path.join(upload_dir, filename)

    if not os.path.isfile(file_path):
        raise HTTPException(404, f"File '{filename}' not found")

    os.remove(file_path)
    logger.info(f"Deleted {file_path}")
    return {"message": f"File '{filename}' deleted"}


@router.delete("/{pipeline_id}/files")
def delete_all_uploaded_files(
    pipeline_id: UUID,
    db: Session = Depends(get_db),
):
    """Delete all uploaded files for a pipeline."""
    pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
    if not pipeline:
        raise HTTPException(404, "Pipeline not found")

    upload_dir = _get_upload_dir(pipeline_id)
    if os.path.isdir(upload_dir):
        shutil.rmtree(upload_dir)
        os.makedirs(upload_dir, exist_ok=True)
        logger.info(f"Cleared all files in {upload_dir}")

    return {"message": "All files deleted"}
