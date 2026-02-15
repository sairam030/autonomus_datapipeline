"""
Application configuration using Pydantic Settings.
Reads from environment variables (set in docker-compose).
"""

from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Application
    app_name: str = "Autonomous Pipeline"
    debug: bool = True

    # PostgreSQL
    database_url: str = "postgresql+psycopg2://pipeline:pipeline123@localhost:5433/autonomous_pipeline"

    # MinIO / S3
    minio_endpoint: str = "http://localhost:9010"
    aws_access_key_id: str = "minioadmin"
    aws_secret_access_key: str = "minioadmin"

    # Buckets
    bronze_bucket: str = "bronze"
    silver_bucket: str = "silver"
    gold_bucket: str = "gold"
    reference_bucket: str = "reference"
    temp_bucket: str = "temp"

    # Redis
    redis_url: str = "redis://localhost:6380/0"

    # Spark
    spark_master_url: str = "local[*]"

    # Data mount (inside container, user data gets mounted here)
    data_mount_path: str = "/data/pipeline"

    # Schema detection
    schema_sample_rows: int = 1000
    schema_min_confidence: float = 0.7

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()
