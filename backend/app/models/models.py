"""
SQLAlchemy ORM models matching the db/init.sql schema.
"""

import uuid
from datetime import datetime
from sqlalchemy import (
    Column, String, Text, Integer, BigInteger, Float, Boolean,
    DateTime, ForeignKey, UniqueConstraint, Index, CheckConstraint
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from backend.app.database import Base


class Pipeline(Base):
    __tablename__ = "pipelines"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False, unique=True)
    description = Column(Text)
    source_type = Column(String(50), nullable=False)
    status = Column(String(50), nullable=False, default="draft")
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)
    metadata_ = Column("metadata", JSONB, default={})

    # Relationships
    data_source = relationship("DataSource", back_populates="pipeline", uselist=False, cascade="all, delete-orphan")
    schemas = relationship("SchemaRegistry", back_populates="pipeline", cascade="all, delete-orphan")
    bronze_ingestions = relationship("BronzeIngestion", back_populates="pipeline", cascade="all, delete-orphan")
    transformation_modules = relationship("TransformationModule", back_populates="pipeline", cascade="all, delete-orphan")
    executions = relationship("PipelineExecution", back_populates="pipeline", cascade="all, delete-orphan")
    silver_transformations = relationship("SilverTransformation", back_populates="pipeline", cascade="all, delete-orphan")
    transformation_pipelines = relationship("TransformationPipeline", cascade="all, delete-orphan", passive_deletes=True)
    silver_executions = relationship("SilverExecution", cascade="all, delete-orphan", passive_deletes=True)
    gold_transformations = relationship("GoldTransformation", back_populates="pipeline", cascade="all, delete-orphan")
    gold_executions = relationship("GoldExecution", cascade="all, delete-orphan", passive_deletes=True)
    postgres_pushes = relationship("PostgresPush", cascade="all, delete-orphan", passive_deletes=True)


class DataSource(Base):
    __tablename__ = "data_sources"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False)
    source_type = Column(String(50), nullable=False)
    # Local files
    file_path = Column(Text)
    file_format = Column(String(20))
    # API
    api_endpoint = Column(Text)
    api_method = Column(String(10))
    api_headers = Column(JSONB, default={})
    api_body = Column(JSONB, default={})
    api_auth_type = Column(String(50))
    api_credentials = Column(JSONB, default={})
    # Kafka
    kafka_bootstrap = Column(Text)
    kafka_topic = Column(String(255))
    kafka_group_id = Column(String(255))
    kafka_config = Column(JSONB, default={})
    # Database
    db_connection = Column(Text)
    db_query = Column(Text)
    db_table = Column(String(255))
    # Common
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    config = Column(JSONB, default={})

    pipeline = relationship("Pipeline", back_populates="data_source")


class SchemaRegistry(Base):
    __tablename__ = "schema_registry"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False)
    version = Column(Integer, nullable=False, default=1)
    # Detected schema
    fields = Column(JSONB, nullable=False)
    # Detection metadata
    total_files = Column(Integer, default=0)
    compatible_files = Column(JSONB, default=[])
    incompatible_files = Column(JSONB, default=[])
    sample_row_count = Column(Integer, default=0)
    detection_confidence = Column(Float, default=0.0)
    # User modifications
    user_modified = Column(Boolean, default=False)
    user_overrides = Column(JSONB, default={})
    # Status
    status = Column(String(50), default="detected")
    confirmed_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (UniqueConstraint("pipeline_id", "version"),)

    pipeline = relationship("Pipeline", back_populates="schemas")


class BronzeIngestion(Base):
    __tablename__ = "bronze_ingestions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False)
    schema_version = Column(Integer, nullable=False)
    files_ingested = Column(JSONB, default=[])
    files_skipped = Column(JSONB, default=[])
    bronze_path = Column(Text, nullable=False)
    output_format = Column(String(20), default="parquet")
    partition_by = Column(String(255), default="ingestion_date")
    total_records = Column(BigInteger, default=0)
    total_size_bytes = Column(BigInteger, default=0)
    duration_seconds = Column(Float, default=0)
    status = Column(String(50), default="running")
    error_message = Column(Text)
    started_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    completed_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    pipeline = relationship("Pipeline", back_populates="bronze_ingestions")


class TransformationModule(Base):
    __tablename__ = "transformation_modules"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False)
    name = Column(String(255), nullable=False, unique=True)
    module_type = Column(String(50), nullable=False)
    description = Column(Text)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    pipeline = relationship("Pipeline", back_populates="transformation_modules")
    versions = relationship("TransformationVersion", back_populates="module", cascade="all, delete-orphan")


class TransformationVersion(Base):
    __tablename__ = "transformation_versions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    module_id = Column(UUID(as_uuid=True), ForeignKey("transformation_modules.id", ondelete="CASCADE"), nullable=False)
    version = Column(Integer, nullable=False, default=1)
    config = Column(JSONB, nullable=False)
    is_active = Column(Boolean, default=False)
    activated_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (UniqueConstraint("module_id", "version"),)

    module = relationship("TransformationModule", back_populates="versions")


class TransformationPipeline(Base):
    __tablename__ = "transformation_pipelines"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False)
    version = Column(Integer, nullable=False, default=1)
    name = Column(String(255))
    modules = Column(JSONB, nullable=False)
    is_active = Column(Boolean, default=False)
    scheduled_activation = Column(DateTime(timezone=True))
    activated_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (UniqueConstraint("pipeline_id", "version"),)


class PipelineExecution(Base):
    __tablename__ = "pipeline_executions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False)
    transformation_pipeline_version = Column(Integer)
    execution_type = Column(String(50), default="batch")
    status = Column(String(50), default="running")
    input_records = Column(BigInteger, default=0)
    output_records = Column(BigInteger, default=0)
    filtered_records = Column(BigInteger, default=0)
    failed_records = Column(BigInteger, default=0)
    duration_seconds = Column(Float, default=0)
    module_metrics = Column(JSONB, default=[])
    error_module = Column(String(255))
    error_message = Column(Text)
    error_details = Column(JSONB)
    started_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    completed_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    pipeline = relationship("Pipeline", back_populates="executions")


class ApiConnector(Base):
    __tablename__ = "api_connectors"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipelines.id", ondelete="SET NULL"))
    name = Column(String(255), nullable=False, unique=True)
    base_url = Column(Text, nullable=False)
    auth_type = Column(String(50))
    auth_config = Column(JSONB, default={})
    headers = Column(JSONB, default={})
    rate_limit = Column(Integer, default=100)
    timeout_seconds = Column(Integer, default=30)
    retry_count = Column(Integer, default=3)
    cache_ttl = Column(Integer, default=3600)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)


class ReferenceTable(Base):
    __tablename__ = "reference_tables"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipelines.id", ondelete="SET NULL"))
    name = Column(String(255), nullable=False, unique=True)
    storage_path = Column(Text, nullable=False)
    file_format = Column(String(20), default="parquet")
    schema = Column(JSONB)
    row_count = Column(BigInteger, default=0)
    uploaded_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)


class AuditLog(Base):
    __tablename__ = "audit_log"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_type = Column(String(100), nullable=False)
    entity_id = Column(UUID(as_uuid=True), nullable=False)
    action = Column(String(50), nullable=False)
    old_value = Column(JSONB)
    new_value = Column(JSONB)
    performed_at = Column(DateTime(timezone=True), default=datetime.utcnow)


class SilverTransformation(Base):
    __tablename__ = "silver_transformations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    # AI-generated PySpark code
    generated_code = Column(Text)
    confirmed_code = Column(Text)
    # Schema context
    input_schema = Column(JSONB, default=[])
    output_schema = Column(JSONB, default=[])
    sample_input = Column(JSONB, default=[])
    sample_output = Column(JSONB, default=[])
    # Status
    status = Column(String(50), default="draft")
    conversation_count = Column(Integer, default=0)
    task_order = Column(Integer, default=1)
    version = Column(Integer, default=1)
    is_active = Column(Boolean, default=True)
    # Timestamps
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    pipeline = relationship("Pipeline", back_populates="silver_transformations")
    messages = relationship("ConversationMessage", back_populates="transformation",
                          cascade="all, delete-orphan", order_by="ConversationMessage.message_order")


class ConversationMessage(Base):
    __tablename__ = "conversation_messages"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    transformation_id = Column(UUID(as_uuid=True), ForeignKey("silver_transformations.id", ondelete="CASCADE"), nullable=False)
    role = Column(String(20), nullable=False)  # user, assistant, system
    content = Column(Text, nullable=False)
    code_block = Column(Text)
    dry_run_result = Column(JSONB)
    message_order = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    transformation = relationship("SilverTransformation", back_populates="messages")


class SilverExecution(Base):
    __tablename__ = "silver_executions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False)
    transformation_ids = Column(JSONB, default=[])
    status = Column(String(50), default="running")
    input_path = Column(Text)
    output_path = Column(Text)
    input_records = Column(BigInteger, default=0)
    output_records = Column(BigInteger, default=0)
    duration_seconds = Column(Float, default=0)
    error_message = Column(Text)
    started_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    completed_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)


# ============================================================================
# Gold Layer Models
# ============================================================================

class GoldTransformation(Base):
    __tablename__ = "gold_transformations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    # AI-generated PySpark code
    generated_code = Column(Text)
    confirmed_code = Column(Text)
    # Schema context (from silver output)
    input_schema = Column(JSONB, default=[])
    output_schema = Column(JSONB, default=[])
    sample_input = Column(JSONB, default=[])
    sample_output = Column(JSONB, default=[])
    # Status
    status = Column(String(50), default="draft")
    conversation_count = Column(Integer, default=0)
    task_order = Column(Integer, default=1)
    version = Column(Integer, default=1)
    is_active = Column(Boolean, default=True)
    # Timestamps
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    pipeline = relationship("Pipeline", back_populates="gold_transformations")
    messages = relationship("GoldConversationMessage", back_populates="transformation",
                          cascade="all, delete-orphan", order_by="GoldConversationMessage.message_order")


class GoldConversationMessage(Base):
    __tablename__ = "gold_conversation_messages"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    transformation_id = Column(UUID(as_uuid=True), ForeignKey("gold_transformations.id", ondelete="CASCADE"), nullable=False)
    role = Column(String(20), nullable=False)
    content = Column(Text, nullable=False)
    code_block = Column(Text)
    dry_run_result = Column(JSONB)
    message_order = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    transformation = relationship("GoldTransformation", back_populates="messages")


class GoldExecution(Base):
    __tablename__ = "gold_executions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False)
    transformation_ids = Column(JSONB, default=[])
    status = Column(String(50), default="running")
    input_path = Column(Text)
    output_path = Column(Text)
    input_records = Column(BigInteger, default=0)
    output_records = Column(BigInteger, default=0)
    duration_seconds = Column(Float, default=0)
    error_message = Column(Text)
    started_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    completed_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)


class PostgresPush(Base):
    __tablename__ = "postgres_pushes"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False)
    gold_execution_id = Column(UUID(as_uuid=True), ForeignKey("gold_executions.id", ondelete="SET NULL"))
    table_name = Column(String(255), nullable=False)
    status = Column(String(50), default="running")
    records_pushed = Column(BigInteger, default=0)
    duration_seconds = Column(Float, default=0)
    error_message = Column(Text)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    completed_at = Column(DateTime(timezone=True))
