-- init-db.sql: Bootstrap script for PostgreSQL container
-- This runs automatically via docker-entrypoint-initdb.d

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Create schemas table
CREATE TABLE IF NOT EXISTS schemas (
    schema_id   TEXT PRIMARY KEY DEFAULT 'sch_' || gen_random_uuid()::text,
    name        TEXT NOT NULL,
    namespace   TEXT NOT NULL DEFAULT 'default',
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by  TEXT NOT NULL,
    deleted_at  TIMESTAMPTZ,
    UNIQUE(namespace, name)
);

-- Create schema_versions table
CREATE TABLE IF NOT EXISTS schema_versions (
    version_id         TEXT PRIMARY KEY DEFAULT 'sv_' || gen_random_uuid()::text,
    schema_id          TEXT NOT NULL REFERENCES schemas(schema_id),
    version            INTEGER NOT NULL,
    definition         JSONB NOT NULL,
    fingerprint        TEXT NOT NULL,
    compatibility_mode TEXT NOT NULL DEFAULT 'BACKWARD',
    status             TEXT NOT NULL DEFAULT 'active',
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by         TEXT NOT NULL,
    UNIQUE(schema_id, version),
    UNIQUE(schema_id, fingerprint)
);

-- Create streams table
CREATE TABLE IF NOT EXISTS streams (
    stream_id        TEXT PRIMARY KEY DEFAULT 'str_' || gen_random_uuid()::text,
    name             TEXT NOT NULL UNIQUE,
    description      TEXT,
    schema_id        TEXT NOT NULL REFERENCES schemas(schema_id),
    status           TEXT NOT NULL DEFAULT 'creating',
    retention_policy JSONB,
    partition_config JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by       TEXT NOT NULL,
    deleted_at       TIMESTAMPTZ
);

-- Create stream_stats table
CREATE TABLE IF NOT EXISTS stream_stats (
    stream_id        TEXT NOT NULL REFERENCES streams(stream_id),
    stat_date        DATE NOT NULL DEFAULT CURRENT_DATE,
    records_ingested BIGINT NOT NULL DEFAULT 0,
    bytes_ingested   BIGINT NOT NULL DEFAULT 0,
    records_queried  BIGINT NOT NULL DEFAULT 0,
    storage_bytes    BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (stream_id, stat_date)
);

-- Create pipelines table
CREATE TABLE IF NOT EXISTS pipelines (
    pipeline_id  TEXT PRIMARY KEY DEFAULT 'pip_' || gen_random_uuid()::text,
    name         TEXT NOT NULL,
    stream_id    TEXT NOT NULL REFERENCES streams(stream_id),
    status       TEXT NOT NULL DEFAULT 'active',
    dlq_stream_id TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at   TIMESTAMPTZ
);

-- Create pipeline_stages table
CREATE TABLE IF NOT EXISTS pipeline_stages (
    stage_id    TEXT PRIMARY KEY DEFAULT 'stg_' || gen_random_uuid()::text,
    pipeline_id TEXT NOT NULL REFERENCES pipelines(pipeline_id) ON DELETE CASCADE,
    stage_order INTEGER NOT NULL,
    stage_type  TEXT NOT NULL,
    config      JSONB NOT NULL DEFAULT '{}',
    enabled     BOOLEAN NOT NULL DEFAULT true,
    UNIQUE(pipeline_id, stage_order)
);

-- Create component_health table
CREATE TABLE IF NOT EXISTS component_health (
    component_id   TEXT PRIMARY KEY,
    node_id        TEXT NOT NULL,
    component_type TEXT NOT NULL,
    status         TEXT NOT NULL,
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    details        JSONB
);

-- Create system_config table
CREATE TABLE IF NOT EXISTS system_config (
    config_key   TEXT PRIMARY KEY,
    config_value JSONB NOT NULL,
    description  TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by   TEXT NOT NULL DEFAULT 'system'
);

-- Create api_keys table
CREATE TABLE IF NOT EXISTS api_keys (
    key_id             TEXT PRIMARY KEY DEFAULT 'key_' || gen_random_uuid()::text,
    key_hash           TEXT NOT NULL UNIQUE,
    name               TEXT NOT NULL,
    role               TEXT NOT NULL DEFAULT 'reader',
    stream_permissions JSONB DEFAULT '[]',
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at         TIMESTAMPTZ,
    last_used_at       TIMESTAMPTZ,
    created_by         TEXT NOT NULL DEFAULT 'system'
);

-- Create audit_log table (append-only)
CREATE TABLE IF NOT EXISTS audit_log (
    log_id        TEXT PRIMARY KEY DEFAULT 'log_' || gen_random_uuid()::text,
    timestamp     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    actor         TEXT NOT NULL,
    action        TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id   TEXT,
    details       JSONB,
    ip_address    TEXT,
    trace_id      TEXT
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_schema_versions_schema_id ON schema_versions(schema_id);
CREATE INDEX IF NOT EXISTS idx_streams_schema_id ON streams(schema_id);
CREATE INDEX IF NOT EXISTS idx_streams_status ON streams(status);
CREATE INDEX IF NOT EXISTS idx_pipelines_stream_id ON pipelines(stream_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp ON audit_log(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_resource ON audit_log(resource_type, resource_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_key_hash ON api_keys(key_hash);

-- Insert default system config
INSERT INTO system_config (config_key, config_value, description, updated_by)
VALUES
    ('ingestion.max_record_size_bytes', '4194304', 'Max record size (4MB)', 'system'),
    ('ingestion.max_batch_size', '10000', 'Max batch size', 'system'),
    ('query.default_limit', '100', 'Default query limit', 'system'),
    ('query.max_limit', '10000', 'Max query limit', 'system'),
    ('retention.default_days', '30', 'Default retention in days', 'system')
ON CONFLICT (config_key) DO NOTHING;
