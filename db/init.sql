-- =============================================================================
-- AUTONOMOUS PIPELINE - Database Schema
-- PostgreSQL 15 initialization script
-- =============================================================================

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- =============================================================================
-- 1. PIPELINES - Main pipeline configuration
-- =============================================================================
CREATE TABLE pipelines (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    source_type     VARCHAR(50) NOT NULL CHECK (source_type IN ('csv', 'json', 'parquet', 'api', 'kafka', 'database')),
    status          VARCHAR(50) NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'schema_detected', 'schema_confirmed', 'bronze_ready', 'silver_configured', 'active', 'paused', 'error')),
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    metadata        JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX idx_pipelines_status ON pipelines(status);
CREATE INDEX idx_pipelines_source_type ON pipelines(source_type);

-- =============================================================================
-- 2. DATA SOURCES - Connection & path details
-- =============================================================================
CREATE TABLE data_sources (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    source_type     VARCHAR(50) NOT NULL,
    -- For local files: directory path + file format
    file_path       TEXT,
    file_format     VARCHAR(20),
    -- For API sources
    api_endpoint    TEXT,
    api_method      VARCHAR(10),
    api_headers     JSONB DEFAULT '{}'::jsonb,
    api_body        JSONB DEFAULT '{}'::jsonb,
    api_auth_type   VARCHAR(50),
    api_credentials JSONB DEFAULT '{}'::jsonb,
    -- For Kafka sources
    kafka_bootstrap TEXT,
    kafka_topic     VARCHAR(255),
    kafka_group_id  VARCHAR(255),
    kafka_config    JSONB DEFAULT '{}'::jsonb,
    -- For database sources
    db_connection   TEXT,
    db_query        TEXT,
    db_table        VARCHAR(255),
    -- Common
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    config          JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX idx_data_sources_pipeline ON data_sources(pipeline_id);

-- =============================================================================
-- 3. SCHEMA REGISTRY - Auto-detected schemas with versioning
-- =============================================================================
CREATE TABLE schema_registry (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    version         INTEGER NOT NULL DEFAULT 1,
    -- Detected schema
    fields          JSONB NOT NULL,  -- [{name, type, nullable, confidence, sample_values, unique_count, null_count}]
    -- Detection metadata
    total_files     INTEGER DEFAULT 0,
    compatible_files JSONB DEFAULT '[]'::jsonb,     -- List of files that match schema
    incompatible_files JSONB DEFAULT '[]'::jsonb,   -- Files with schema mismatch + reason
    sample_row_count INTEGER DEFAULT 0,
    detection_confidence FLOAT DEFAULT 0.0,
    -- User modifications
    user_modified   BOOLEAN DEFAULT FALSE,
    user_overrides  JSONB DEFAULT '{}'::jsonb,      -- What the user changed from detected
    -- Status
    status          VARCHAR(50) DEFAULT 'detected' CHECK (status IN ('detected', 'confirmed', 'superseded')),
    confirmed_at    TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(pipeline_id, version)
);

CREATE INDEX idx_schema_registry_pipeline ON schema_registry(pipeline_id);
CREATE INDEX idx_schema_registry_status ON schema_registry(status);

-- =============================================================================
-- 4. BRONZE INGESTIONS - Track each ingestion into Bronze layer
-- =============================================================================
CREATE TABLE bronze_ingestions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    schema_version  INTEGER NOT NULL,
    -- Files processed
    files_ingested  JSONB DEFAULT '[]'::jsonb,      -- [{path, size, rows, status}]
    files_skipped   JSONB DEFAULT '[]'::jsonb,       -- [{path, reason}]
    -- Storage details
    bronze_path     TEXT NOT NULL,                    -- s3a://bronze/{pipeline_id}/v{schema_version}/...
    output_format   VARCHAR(20) DEFAULT 'parquet',
    partition_by    VARCHAR(255) DEFAULT 'ingestion_date',
    -- Metrics
    total_records   BIGINT DEFAULT 0,
    total_size_bytes BIGINT DEFAULT 0,
    duration_seconds FLOAT DEFAULT 0,
    -- Status
    status          VARCHAR(50) DEFAULT 'running' CHECK (status IN ('running', 'success', 'failed', 'partial')),
    error_message   TEXT,
    started_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at    TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_bronze_ingestions_pipeline ON bronze_ingestions(pipeline_id);
CREATE INDEX idx_bronze_ingestions_status ON bronze_ingestions(status);

-- =============================================================================
-- 5. TRANSFORMATION MODULES - Reusable transformation building blocks
-- =============================================================================
CREATE TABLE transformation_modules (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    module_type     VARCHAR(50) NOT NULL CHECK (module_type IN (
        'filter', 'drop_fields', 'add_field_calculated', 'add_field_api_enrich',
        'add_field_join_reference', 'add_field_join_bronze', 'add_field_custom_function',
        'validate', 'rename_fields', 'cast_types'
    )),
    description     TEXT,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_transformation_modules_pipeline ON transformation_modules(pipeline_id);

-- =============================================================================
-- 6. TRANSFORMATION VERSIONS - Version control for each module
-- =============================================================================
CREATE TABLE transformation_versions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    module_id       UUID NOT NULL REFERENCES transformation_modules(id) ON DELETE CASCADE,
    version         INTEGER NOT NULL DEFAULT 1,
    config          JSONB NOT NULL,           -- Module-specific configuration
    is_active       BOOLEAN DEFAULT FALSE,
    activated_at    TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(module_id, version)
);

CREATE INDEX idx_transformation_versions_module ON transformation_versions(module_id);
CREATE INDEX idx_transformation_versions_active ON transformation_versions(is_active) WHERE is_active = TRUE;

-- =============================================================================
-- 7. TRANSFORMATION PIPELINES - Complete transformation workflows
-- =============================================================================
CREATE TABLE transformation_pipelines (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    version         INTEGER NOT NULL DEFAULT 1,
    name            VARCHAR(255),
    -- Ordered list of module+version references
    modules         JSONB NOT NULL,   -- [{module_id, version_id, execution_order}]
    -- Scheduling
    is_active       BOOLEAN DEFAULT FALSE,
    scheduled_activation TIMESTAMP WITH TIME ZONE,
    activated_at    TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(pipeline_id, version)
);

CREATE INDEX idx_transformation_pipelines_pipeline ON transformation_pipelines(pipeline_id);

-- =============================================================================
-- 8. API CONNECTORS - API configuration for enrichment
-- =============================================================================
CREATE TABLE api_connectors (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id     UUID REFERENCES pipelines(id) ON DELETE SET NULL,
    name            VARCHAR(255) NOT NULL,
    base_url        TEXT NOT NULL,
    auth_type       VARCHAR(50),
    auth_config     JSONB DEFAULT '{}'::jsonb,
    headers         JSONB DEFAULT '{}'::jsonb,
    rate_limit      INTEGER DEFAULT 100,          -- requests per minute
    timeout_seconds INTEGER DEFAULT 30,
    retry_count     INTEGER DEFAULT 3,
    cache_ttl       INTEGER DEFAULT 3600,         -- seconds
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- =============================================================================
-- 9. REFERENCE TABLES - Lookup data management
-- =============================================================================
CREATE TABLE reference_tables (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id     UUID REFERENCES pipelines(id) ON DELETE SET NULL,
    name            VARCHAR(255) NOT NULL,
    storage_path    TEXT NOT NULL,                 -- MinIO path
    file_format     VARCHAR(20) DEFAULT 'parquet',
    schema          JSONB,                        -- Field definitions
    row_count       BIGINT DEFAULT 0,
    uploaded_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- =============================================================================
-- 10. USER FUNCTIONS - Custom Python code
-- =============================================================================
CREATE TABLE user_functions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id     UUID REFERENCES pipelines(id) ON DELETE SET NULL,
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    function_code   TEXT NOT NULL,
    input_fields    JSONB NOT NULL,               -- [{name, type}]
    output_type     VARCHAR(50) NOT NULL,
    test_cases      JSONB DEFAULT '[]'::jsonb,    -- [{input, expected_output}]
    is_validated    BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- =============================================================================
-- 11. PIPELINE EXECUTIONS - Execution tracking
-- =============================================================================
CREATE TABLE pipeline_executions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    transformation_pipeline_version INTEGER,
    -- Execution details
    execution_type  VARCHAR(50) DEFAULT 'batch' CHECK (execution_type IN ('batch', 'stream', 'manual')),
    status          VARCHAR(50) DEFAULT 'running' CHECK (status IN ('running', 'success', 'failed', 'cancelled')),
    -- Metrics
    input_records   BIGINT DEFAULT 0,
    output_records  BIGINT DEFAULT 0,
    filtered_records BIGINT DEFAULT 0,
    failed_records  BIGINT DEFAULT 0,
    duration_seconds FLOAT DEFAULT 0,
    -- Module-level metrics
    module_metrics  JSONB DEFAULT '[]'::jsonb,
    -- Error details
    error_module    VARCHAR(255),
    error_message   TEXT,
    error_details   JSONB,
    -- Timestamps
    started_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at    TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_pipeline_executions_pipeline ON pipeline_executions(pipeline_id);
CREATE INDEX idx_pipeline_executions_status ON pipeline_executions(status);

-- =============================================================================
-- 12. DATA QUALITY RESULTS - Validation results
-- =============================================================================
CREATE TABLE data_quality_results (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    execution_id    UUID NOT NULL REFERENCES pipeline_executions(id) ON DELETE CASCADE,
    module_id       UUID REFERENCES transformation_modules(id),
    rule_name       VARCHAR(255),
    rule_config     JSONB,
    passed          BOOLEAN,
    total_records   BIGINT DEFAULT 0,
    passed_records  BIGINT DEFAULT 0,
    failed_records  BIGINT DEFAULT 0,
    failure_samples JSONB DEFAULT '[]'::jsonb,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_data_quality_execution ON data_quality_results(execution_id);

-- =============================================================================
-- 13. API CACHE - API response caching
-- =============================================================================
CREATE TABLE api_cache (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connector_id    UUID NOT NULL REFERENCES api_connectors(id) ON DELETE CASCADE,
    cache_key       VARCHAR(512) NOT NULL,
    request_params  JSONB,
    response_data   JSONB NOT NULL,
    cached_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    hit_count       INTEGER DEFAULT 0
);

CREATE UNIQUE INDEX idx_api_cache_key ON api_cache(connector_id, cache_key);
CREATE INDEX idx_api_cache_expires ON api_cache(expires_at);

-- =============================================================================
-- 14. AUDIT LOG - Full audit trail
-- =============================================================================
CREATE TABLE audit_log (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_type     VARCHAR(100) NOT NULL,
    entity_id       UUID NOT NULL,
    action          VARCHAR(50) NOT NULL,
    old_value       JSONB,
    new_value       JSONB,
    performed_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_audit_log_entity ON audit_log(entity_type, entity_id);
CREATE INDEX idx_audit_log_time ON audit_log(performed_at);

-- =============================================================================
-- HELPER FUNCTIONS
-- =============================================================================

-- Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_pipelines_updated_at
    BEFORE UPDATE ON pipelines
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_transformation_modules_updated_at
    BEFORE UPDATE ON transformation_modules
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_reference_tables_updated_at
    BEFORE UPDATE ON reference_tables
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- Clean expired API cache
CREATE OR REPLACE FUNCTION clean_expired_cache()
RETURNS void AS $$
BEGIN
    DELETE FROM api_cache WHERE expires_at < NOW();
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SEED: Alert configurations table (for future use)
-- =============================================================================
CREATE TABLE alert_configurations (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id     UUID REFERENCES pipelines(id) ON DELETE CASCADE,
    alert_type      VARCHAR(50) NOT NULL CHECK (alert_type IN ('email', 'slack', 'webhook')),
    config          JSONB NOT NULL,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- =============================================================================
-- DONE
-- =============================================================================

-- =============================================================================
-- 15. SILVER TRANSFORMATIONS - AI-generated transformation code
-- =============================================================================
CREATE TABLE silver_transformations (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    -- AI-generated PySpark code
    generated_code  TEXT,
    confirmed_code  TEXT,                             -- User-reviewed/edited final code
    -- Schema context
    input_schema    JSONB DEFAULT '[]'::jsonb,        -- Bronze schema fields
    output_schema   JSONB DEFAULT '[]'::jsonb,        -- Expected output schema after transform
    sample_input    JSONB DEFAULT '[]'::jsonb,        -- 5 sample rows from bronze
    sample_output   JSONB DEFAULT '[]'::jsonb,        -- Dry-run output rows
    -- Status tracking
    status          VARCHAR(50) DEFAULT 'draft' CHECK (status IN (
        'draft', 'chatting', 'code_generated', 'code_reviewed', 'dry_run_passed', 'confirmed', 'error'
    )),
    -- Conversation tracking
    conversation_count INTEGER DEFAULT 0,
    -- Ordering within project
    task_order      INTEGER DEFAULT 1,
    -- Timestamps
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_silver_transformations_pipeline ON silver_transformations(pipeline_id);
CREATE INDEX idx_silver_transformations_status ON silver_transformations(status);

CREATE TRIGGER trg_silver_transformations_updated_at
    BEFORE UPDATE ON silver_transformations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- =============================================================================
-- 16. CONVERSATION MESSAGES - Chat history for AI-driven transformations
-- =============================================================================
CREATE TABLE conversation_messages (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transformation_id UUID NOT NULL REFERENCES silver_transformations(id) ON DELETE CASCADE,
    role            VARCHAR(20) NOT NULL CHECK (role IN ('user', 'assistant', 'system')),
    content         TEXT NOT NULL,
    -- Optional: code block extracted from assistant response
    code_block      TEXT,
    -- Optional: dry-run results attached to this message
    dry_run_result  JSONB,
    -- Message ordering
    message_order   INTEGER NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_conversation_messages_transformation ON conversation_messages(transformation_id);
CREATE INDEX idx_conversation_messages_order ON conversation_messages(transformation_id, message_order);

