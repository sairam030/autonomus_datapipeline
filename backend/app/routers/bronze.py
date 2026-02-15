"""
Bronze ingestion status/tracking API routes.
"""

import logging
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from backend.app.database import get_db
from backend.app.models.models import BronzeIngestion
from backend.app.schemas.bronze import BronzeIngestionResponse, BronzeIngestionStatus

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/bronze", tags=["Bronze Ingestion"])


@router.get("/{pipeline_id}/ingestions", response_model=list[BronzeIngestionResponse])
def list_ingestions(pipeline_id: UUID, db: Session = Depends(get_db)):
    """List all Bronze ingestion runs for a pipeline."""
    ingestions = (
        db.query(BronzeIngestion)
        .filter(BronzeIngestion.pipeline_id == pipeline_id)
        .order_by(BronzeIngestion.created_at.desc())
        .all()
    )
    return ingestions


@router.get("/ingestion/{ingestion_id}", response_model=BronzeIngestionResponse)
def get_ingestion(ingestion_id: UUID, db: Session = Depends(get_db)):
    """Get details of a specific Bronze ingestion run."""
    ingestion = db.query(BronzeIngestion).filter(BronzeIngestion.id == ingestion_id).first()
    if not ingestion:
        raise HTTPException(404, "Ingestion not found")
    return ingestion


@router.get("/ingestion/{ingestion_id}/status", response_model=BronzeIngestionStatus)
def get_ingestion_status(ingestion_id: UUID, db: Session = Depends(get_db)):
    """Lightweight status check for a Bronze ingestion (for polling from UI)."""
    ingestion = db.query(BronzeIngestion).filter(BronzeIngestion.id == ingestion_id).first()
    if not ingestion:
        raise HTTPException(404, "Ingestion not found")

    return BronzeIngestionStatus(
        ingestion_id=ingestion.id,
        pipeline_id=ingestion.pipeline_id,
        status=ingestion.status,
        total_records=ingestion.total_records or 0,
        files_processed=len(ingestion.files_ingested or []),
        files_skipped=len(ingestion.files_skipped or []),
        duration_seconds=ingestion.duration_seconds or 0,
        error_message=ingestion.error_message,
    )
