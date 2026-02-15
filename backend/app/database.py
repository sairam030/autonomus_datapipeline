"""
SQLAlchemy database setup - session factory and dependency injection.
Compatible with SQLAlchemy 1.4+ (same as aviation project).
"""

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from backend.app.config import get_settings

Base = declarative_base()

settings = get_settings()

engine = create_engine(
    settings.database_url,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    echo=settings.debug,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """FastAPI dependency: yields a DB session, closes when done."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
