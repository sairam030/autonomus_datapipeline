"""
Autonomous Pipeline — FastAPI Application Entry Point

Registers all routers and configures CORS for React frontend.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.app.routers import pipelines, schemas, bronze, upload, dags, silver
from backend.app.config import get_settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup/shutdown events."""
    settings = get_settings()
    logger.info("=" * 60)
    logger.info("🚀 Autonomous Pipeline API starting up")
    logger.info(f"   Database: {settings.database_url.split('@')[1] if '@' in settings.database_url else 'configured'}")
    logger.info(f"   MinIO:    {settings.minio_endpoint}")
    logger.info(f"   Spark:    {settings.spark_master_url}")
    logger.info("=" * 60)
    yield
    logger.info("Autonomous Pipeline API shutting down")


app = FastAPI(
    title="Autonomous Pipeline API",
    description=(
        "Generalized, configuration-driven data pipeline system. "
        "Supports Bronze → Silver → Gold medallion architecture with "
        "auto schema detection, versioned transformations, and a visual UI."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

# CORS — allow React dev server
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3001",
        "http://localhost:3000",
        "http://127.0.0.1:3001",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(pipelines.router)
app.include_router(schemas.router)
app.include_router(bronze.router)
app.include_router(upload.router)
app.include_router(dags.router)
app.include_router(silver.router)


@app.get("/", tags=["Health"])
def root():
    return {
        "service": "Autonomous Pipeline API",
        "version": "0.1.0",
        "status": "running",
        "docs": "/docs",
    }


@app.get("/health", tags=["Health"])
def health_check():
    return {"status": "healthy"}
